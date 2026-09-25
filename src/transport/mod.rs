// Project:   scalo
// File:      src/transport/mod.rs
// Purpose:   Transport abstraction layer for message delivery
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # Transport Abstraction Layer
//!
//! Pluggable message transport with split sender/receiver traits for
//! type-safe factory construction and runtime transport selection.
//!
//! ## Architecture
//!
//! ```text
//!                     TransportBase
//!       close(), is_healthy(), name(), healthcheck()
//!              |                            |
//!     TransportSender                TransportReceiver (type Token)
//!       send(destination, payload)     recv(max)
//!       send_batch(records)            recv_limited(limits)
//!                                      commit(tokens)
//!              |                            |
//!              +--- Transport (blanket) ----+
//! ```
//!
//! `send` and `send_batch` return [`SendResult`]; `recv` and `recv_limited`
//! return `TransportResult<WorkBatch<Token>>` (see [`WorkBatch`]).
//!
//! None of these traits is object safe: every async method returns
//! `impl Future`, so `Box<dyn TransportSender>` does not compile. Runtime
//! selection uses enum dispatch instead.
//!
//! - **Output stages** (DLQ, forwarding, archiving): [`AnySender`] when config
//!   picks the backend, or a generic `S: TransportSender`
//! - **Input stages** (receiver, fetcher): [`AnyReceiver`] when config picks the
//!   backend, or a concrete receiver such as `KafkaTransport` when the stage needs
//!   the backend's own token type
//! - **Factory**: `AnySender::from_config(key)` and `AnyReceiver::from_config(key)`
//!   read a [`TransportConfig`] from the cascade; `from_transport_config(&cfg)`
//!   takes one directly
//!
//! ## Transport Selection
//!
//! | Transport | Send | Recv | Use Case |
//! |-----------|------|------|----------|
//! | **Kafka** | Yes | Yes | Production default, PB/day, persistence |
//! | **gRPC** | Yes | Yes | Low-latency direct, service mesh |
//! | **Memory** | Yes | Yes | Unit tests, same-process |
//! | **File** | Yes | Yes | Debugging, audit trails, replay |
//! | **Pipe** | Yes | Yes | Unix pipelines, sidecar pattern |
//! | **HTTP** | Yes | Yes | Webhook delivery, REST ingest |
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::transport::{AnySender, TransportSender};
//!
//! // The factory builds the backend named by `type` under this cascade key
//! let sender = AnySender::from_config("transport.output").await?;
//! let result = sender.send("events.land", payload).await; // SendResult
//! ```

pub mod ack;
pub mod codec;
mod detect;
mod error;
pub mod factory;
pub mod filter;
pub mod finalizer;
pub mod propagation;
mod traits;
mod types;
mod work_batch;

pub use types::PayloadFormat;

// Re-export stateful format detection
pub use detect::{DetectedFormat, FormatDetector, FormatMode, detect_format};

#[cfg(feature = "transport-kafka")]
pub mod kafka;

#[cfg(feature = "transport-grpc")]
pub mod grpc;

#[cfg(feature = "transport-grpc-vector-compat")]
pub mod vector_compat;

#[cfg(feature = "transport-memory")]
pub mod memory;

#[cfg(feature = "transport-pipe")]
pub mod pipe;

#[cfg(feature = "transport-file")]
pub mod file;

#[cfg(feature = "transport-http")]
pub mod http;

pub mod routed;

// Re-exports -- traits and factory
pub use ack::{
    AckControl, AckKind, AcknowledgementsConfig, AcknowledgingReceiver, DeadLetterReason, HeldAcks,
    SinkConfirmation, SourceAck,
};
pub use codec::{CodecError, FieldRef, ParsedPayload, parse};
pub use error::{TransportError, TransportResult};
pub use factory::{AnyReceiver, AnySender, AnyToken};
pub use finalizer::{BatchFinalizer, DeliveryStatus, PieceFinalizer};
pub use routed::RoutedSender;
pub use traits::{
    CommitToken, FromCascade, HealthcheckConfig, RecvBatch, RecvLimits, Transport, TransportBase,
    TransportReceiver, TransportSender, boot_healthcheck,
};
pub use types::{Message, SendResult, TransportConfig, TransportType};
pub use work_batch::{FramingError, Record, RecordCodecError, RecordMeta, WorkBatch};

#[cfg(feature = "transport-kafka")]
pub use kafka::{KafkaConfig, KafkaToken, KafkaTransport};

#[cfg(feature = "transport-grpc")]
pub use grpc::{GrpcConfig, GrpcToken, GrpcTransport, GrpcTransportBuilder};

#[cfg(feature = "transport-grpc-vector-compat")]
pub use vector_compat::{VectorCompatClient, VectorCompatService};

#[cfg(feature = "transport-memory")]
pub use memory::{MemoryConfig, MemoryToken, MemoryTransport};

#[cfg(feature = "transport-pipe")]
pub use pipe::{PipeToken, PipeTransport, PipeTransportConfig};

#[cfg(feature = "transport-file")]
pub use file::{FileToken, FileTransport, FileTransportConfig};

#[cfg(feature = "transport-http")]
pub use http::{HttpToken, HttpTransport, HttpTransportConfig};
