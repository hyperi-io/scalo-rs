// Project:   scalo
// File:      src/http_server/mod.rs
// Purpose:   High-performance HTTP server with axum
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! HTTP server built on axum. Compatible with Tonic for gRPC.
//!
//! ## Features
//!
//! - Graceful shutdown
//! - Configurable request timeout, in-flight cap, Tower middleware
//! - Health endpoints (`/livez` and `/readyz` -- no aliases)
//! - TLS with certificate reload, with the `tls` feature: `tls_cert_path` /
//!   `tls_key_path`, or `HttpServer::with_tls` for any `ServerTls`
//!
//! A `/metrics` endpoint is not wired here. Metrics are served by
//! `MetricsManager` on its own listener.
//!
//! ## Example
//!
//! ```rust,no_run
//! use scalo::http_server::{HttpServer, HttpServerConfig};
//! use axum::{Router, routing::get};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = HttpServerConfig {
//!     bind_address: "0.0.0.0:8080".to_string(),
//!     ..Default::default()
//! };
//!
//! let app = Router::new()
//!     .route("/", get(|| async { "Hello, World!" }));
//!
//! let server = HttpServer::new(config);
//! server.serve(app).await?;
//! # Ok(())
//! # }
//! ```

mod config;
mod error;
mod server;
#[cfg(feature = "tls")]
mod tls;

pub use config::HttpServerConfig;
pub use error::HttpServerError;
pub use server::HttpServer;
#[cfg(feature = "tls")]
pub use tls::TlsListener;

// Re-export axum types users commonly need
pub use axum::{
    Extension, Router,
    extract::{Path, Query, State},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, post, put},
};

/// Result type for HTTP server operations.
pub type Result<T> = std::result::Result<T, HttpServerError>;
