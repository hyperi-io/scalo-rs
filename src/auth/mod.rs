// Project:   scalo
// File:      src/auth/mod.rs
// Purpose:   Shared credential acquisition and placement
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Getting a credential, and putting it on a request.
//!
//! Two halves that compose, and neither knows about the other:
//!
//! - **Acquisition.** An [`Exchange`] obtains one credential, once, over one
//!   protocol. [`Cached`] wraps any exchange and owns the caching, the renewal
//!   point and the single-flight gate, so a cold source hit by a hundred
//!   callers mints once.
//! - **Placement.** A [`HeaderPlacement`], [`QueryPlacement`] or
//!   [`BasicPlacement`] is a [`RequestSigner`](crate::http_client::RequestSigner)
//!   that puts the acquired credential where the provider wants it, per attempt,
//!   on the built request.
//!
//! A list of placements is a tuple -- `(A, B)` is itself a signer -- so a
//! provider wanting two headers from one exchange is two placements over one
//! `Arc` source, with no new type and no `dyn`.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use scalo::auth::{Cached, ClientCredentials, HeaderPlacement};
//! use scalo::http_client::HttpClient;
//! use scalo::sensitive::SensitiveString;
//!
//! # async fn call() -> Result<(), Box<dyn std::error::Error>> {
//! let http = Arc::new(HttpClient::from_cascade()?);
//! let source = Arc::new(Cached::new(
//!     ClientCredentials::new(
//!         Arc::clone(&http),
//!         "https://idp.example/oauth2/token",
//!         "client-42",
//!         SensitiveString::new("resolved-by-the-consumer"),
//!     )
//!     .with_scope("events:read"),
//! ));
//!
//! let response = http
//!     .get_signed(
//!         "https://api.example/v1/events",
//!         &HeaderPlacement::bearer(Arc::clone(&source)),
//!     )
//!     .await?;
//! # let _ = response;
//! # Ok(())
//! # }
//! ```
//!
//! ## What stays with the consumer
//!
//! Everything deployment-shaped and everything with its own crypto. Templating
//! and secret resolution happen before an exchange is built, so every value
//! here is already rendered. A signing scheme -- SigV4, a request HMAC -- is one
//! more implementation of
//! [`RequestSigner`](crate::http_client::RequestSigner) in the consumer that
//! needs it, rather than a signing dependency in every consumer that does not.

pub mod error;
pub mod placement;
pub mod source;

pub use error::AuthError;
pub use placement::{BasicPlacement, HeaderPlacement, QueryPlacement};
pub use source::{
    Cached, ClientCredentials, Credential, CredentialSource, Exchange, MetadataServer, Static,
    TokenPost, TokenReading,
};
