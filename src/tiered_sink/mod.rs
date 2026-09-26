// Project:   scalo
// File:      src/tiered_sink/mod.rs
// Purpose:   Tiered sink with disk spillover for resilient message delivery
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Tiered sink with automatic disk spillover for resilient message delivery.
//!
//! This module provides a wrapper around any async sink (Kafka, S3, HTTP, etc.)
//! that automatically spills messages to disk when the primary sink is unavailable
//! or backpressuring, then drains them back when the sink recovers.
//!
//! ## Design
//!
//! ```text
//!                     +-------------------------------------+
//!                     |           TieredSink                |
//!                     |                                     |
//!    Message -------->|  try_send() to primary sink        |
//!                     |         |                           |
//!                     |         v                           |
//!                     |    +---------+                      |
//!                     |    | Success |--> Done (hot path)   |
//!                     |    +----+----+                      |
//!                     |         | Err(Full/Unavailable)     |
//!                     |         v                           |
//!                     |    +---------+                      |
//!                     |    |  Spool  |--> Disk (cold path)  |
//!                     |    +----+----+                      |
//!                     |         |                           |
//!                     |    Background drain task            |
//!                     |    (when primary recovers)          |
//!                     +-------------------------------------+
//! ```
//!
//! ## Features
//!
//! - **Hot path first**: Always tries primary sink with timeout
//! - **Automatic spillover**: Writes to disk only when primary fails
//! - **Circuit breaker**: Avoids hammering a dead sink
//! - **Background drain**: Recovers spooled messages when sink is healthy
//! - **Configurable ordering**: Interleaved (default) or strict FIFO
//! - **Multiple compression codecs**: Zstd (default, level 1), LZ4, Snappy, None
//!
//! ## Example
//!
//! TieredSink wraps any [`TransportSender`](crate::transport::TransportSender) --
//! the same senders the transport factory produces. Records go straight to the
//! sender on the happy path (no encode); only when the downstream fails are they
//! serialised and spilled to disk, then drained back on recovery.
//!
//! ```rust,ignore
//! use scalo::tiered_sink::{TieredSink, TieredSinkConfig};
//! use scalo::transport::AnySender;
//!
//! let sender = AnySender::from_config("transport.output").await?;
//! let config = TieredSinkConfig::new("/var/spool/myapp.queue");
//! let tiered = TieredSink::new(sender, config).await?;
//!
//! // Automatically spills to disk if the downstream is down, drains on recovery.
//! tiered.send(&record).await?;
//! ```

mod circuit;
mod codec;
mod config;
mod drainer;
mod error;
mod tiered;

pub use circuit::{CircuitBreaker, CircuitState};
pub use codec::CompressionCodec;
pub use config::{DiskAwareConfig, DrainStrategy, OrderingMode, TieredSinkConfig, WhenFull};
// The config's public field and builder take it, and `spool` may be off.
pub use crate::spool_codec::CorruptionPolicy;
pub use error::TieredSinkError;
pub use tiered::TieredSink;

/// Result type for tiered sink operations.
pub type Result<T> = std::result::Result<T, TieredSinkError>;
