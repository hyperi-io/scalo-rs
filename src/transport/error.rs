// Project:   scalo
// File:      src/transport/error.rs
// Purpose:   Transport error types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use thiserror::Error;

/// Result type for transport operations.
pub type TransportResult<T> = Result<T, TransportError>;

/// Errors that can occur during transport operations.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Configuration error (missing or invalid config).
    #[error("transport config error: {0}")]
    Config(String),

    /// Connection error (network, auth, etc.).
    #[error("transport connection error: {0}")]
    Connection(String),

    /// Send operation failed.
    #[error("transport send error: {0}")]
    Send(String),

    /// A record exceeded the message-size ceiling and was refused.
    ///
    /// Distinct from [`Send`](Self::Send) because it is PERMANENT: the same
    /// bytes can never be accepted, so a retry only repeats the loss. The
    /// record belongs in a dead-letter queue. Kafka raises this locally
    /// (librdkafka's `message.max.bytes`) before the broker is consulted, as
    /// well as from the broker's own topic `max.message.bytes`.
    #[error("transport message too large: {bytes} bytes exceeds the message-size ceiling: {detail}")]
    MessageTooLarge {
        /// Payload size that was refused, in bytes.
        bytes: usize,
        /// The underlying client/broker error text.
        detail: String,
    },

    /// Receive operation failed.
    #[error("transport receive error: {0}")]
    Recv(String),

    /// Commit/acknowledge operation failed.
    #[error("transport commit error: {0}")]
    Commit(String),

    /// Transport is closed or shutting down.
    #[error("transport closed")]
    Closed,

    /// Timeout waiting for operation.
    #[error("transport operation timed out")]
    Timeout,

    /// Backpressure -- transport cannot accept more messages.
    #[error("transport backpressure")]
    Backpressure,

    /// Internal transport error.
    #[error("transport internal error: {0}")]
    Internal(String),

    /// Admin operation error (topic/partition management).
    #[error("transport admin error: {0}")]
    Admin(String),
}

impl TransportError {
    /// Returns true if this error is recoverable (retry may succeed).
    #[must_use]
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::Timeout | Self::Backpressure)
    }

    /// Returns true if this error indicates the transport is unusable.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Closed | Self::Config(_))
    }

    /// Returns true if the same payload can never succeed.
    ///
    /// The transport is healthy and the next record may well go through -- it
    /// is THIS record that is poison, so it belongs in a dead-letter queue
    /// rather than a retry loop.
    #[must_use]
    pub fn is_undeliverable(&self) -> bool {
        matches!(self, Self::MessageTooLarge { .. })
    }
}
