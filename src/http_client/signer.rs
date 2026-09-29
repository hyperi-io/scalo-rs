// Project:   scalo
// File:      src/http_client/signer.rs
// Purpose:   Async signing hook run on the built request, inside the retry loop
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The hook that puts a credential on a request.
//!
//! A signer runs on the built [`reqwest::Request`], after the body and query
//! are final and before the request is sent, so a signature can cover them.
//! Because the hook is inside the retry loop it runs again on every attempt: a
//! per-request nonce, a timestamp or a token that expired between attempts is
//! regenerated rather than replayed.
//!
//! The hook is async because acquiring the credential can be I/O -- a token
//! exchange, a metadata-server call, a secret read. The placements in the
//! `auth` module (`auth` feature) are the implementations that come with scalo; a signing
//! scheme with its own crypto dependencies (SigV4, a request HMAC) belongs in
//! the consumer as one more implementation of this trait.

use std::future::Future;

use reqwest::header::HeaderValue;

/// Why a signer could not put its credential on the request.
///
/// Not retried by default: a credential that could not be rendered will not
/// render on the next attempt. A signer whose credential endpoint could not be
/// reached says so with [`Self::retryable`], and the retry loop then re-signs.
/// A placement over an `auth` source (`auth` feature) hands its acquisition failure
/// through whole, so a consumer can read the status of a refusal.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SignError {
    /// The signer's own failure: the credential could not be rendered onto the
    /// request, or a consumer's signing scheme could not run.
    #[error("{message}")]
    Failed {
        /// What could not be done, without the credential in it.
        message: String,
        /// The underlying failure, when there was one.
        #[source]
        cause: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
        /// Whether signing again could succeed.
        retryable: bool,
    },

    /// The credential could not be acquired. Whether another attempt could
    /// succeed is the acquisition failure's own answer.
    #[cfg(feature = "auth")]
    #[cfg_attr(docsrs, doc(cfg(feature = "auth")))]
    #[error("credential unavailable: {0}")]
    Auth(#[from] crate::auth::AuthError),
}

impl SignError {
    /// A signing failure with no underlying error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
            cause: None,
            retryable: false,
        }
    }

    /// A signing failure carrying the error that caused it.
    #[must_use]
    pub fn with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Failed {
            message: message.into(),
            cause: Some(Box::new(cause)),
            retryable: false,
        }
    }

    /// Mark the failure as one that another attempt could get past: a token
    /// endpoint that was unreachable, or did not answer in time. An acquisition
    /// failure already knows, and is left as it is.
    #[must_use]
    pub fn retryable(mut self) -> Self {
        match &mut self {
            Self::Failed { retryable, .. } => *retryable = true,
            #[cfg(feature = "auth")]
            Self::Auth(_) => {}
        }
        self
    }

    /// Whether signing the request again could succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Failed { retryable, .. } => *retryable,
            #[cfg(feature = "auth")]
            Self::Auth(error) => error.is_transient(),
        }
    }
}

/// Puts a credential on a request after it is built and before it is sent.
///
/// Implementations are held by value or behind an `Arc` and passed per call --
/// `async fn` in a trait is not object safe, so there is no `dyn` form. A list
/// of placements is a tuple: `(A, B)`, `(A, B, C)` and `(A, B, C, D)` are
/// signers themselves, applied left to right.
pub trait RequestSigner: Send + Sync {
    /// Put the credential on `request`.
    ///
    /// # Errors
    ///
    /// Returns [`SignError`] when the credential cannot be acquired or cannot
    /// be rendered onto the request.
    fn sign(
        &self,
        request: &mut reqwest::Request,
    ) -> impl Future<Output = Result<(), SignError>> + Send;
}

/// The signer an unsigned call uses, so both paths run one loop.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unsigned;

impl RequestSigner for Unsigned {
    async fn sign(&self, _request: &mut reqwest::Request) -> Result<(), SignError> {
        Ok(())
    }
}

impl<T: RequestSigner + ?Sized> RequestSigner for std::sync::Arc<T> {
    fn sign(
        &self,
        request: &mut reqwest::Request,
    ) -> impl Future<Output = Result<(), SignError>> + Send {
        T::sign(self, request)
    }
}

impl<T: RequestSigner + ?Sized> RequestSigner for &T {
    fn sign(
        &self,
        request: &mut reqwest::Request,
    ) -> impl Future<Output = Result<(), SignError>> + Send {
        T::sign(self, request)
    }
}

impl<A: RequestSigner, B: RequestSigner> RequestSigner for (A, B) {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        self.0.sign(request).await?;
        self.1.sign(request).await
    }
}

impl<A: RequestSigner, B: RequestSigner, C: RequestSigner> RequestSigner for (A, B, C) {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        self.0.sign(request).await?;
        self.1.sign(request).await?;
        self.2.sign(request).await
    }
}

impl<A: RequestSigner, B: RequestSigner, C: RequestSigner, D: RequestSigner> RequestSigner
    for (A, B, C, D)
{
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        self.0.sign(request).await?;
        self.1.sign(request).await?;
        self.2.sign(request).await?;
        self.3.sign(request).await
    }
}

/// The `Authorization` value for HTTP basic auth, marked sensitive so the
/// `http` crate's own `Debug` renders it as `Sensitive` rather than the
/// credential.
///
/// reqwest's encoder is private and only reachable from a `RequestBuilder`,
/// and a signer runs against a built request.
///
/// # Errors
///
/// Returns [`SignError`] when the username carries a colon, which is the
/// field separator RFC 7617 gives the username no way to escape, or when the
/// encoding will not fit a header value.
#[cfg_attr(
    not(any(feature = "auth", feature = "geoip-download")),
    allow(dead_code)
)]
pub(crate) fn basic_auth_value(
    username: &str,
    password: Option<&str>,
) -> Result<HeaderValue, SignError> {
    use base64::Engine as _;

    if username.contains(':') {
        // The colon separates the two halves, so a username carrying one moves
        // the split and authenticates as something other than what was asked.
        return Err(SignError::new("basic auth username cannot contain a colon"));
    }

    let mut raw = String::with_capacity(username.len() + 1 + password.unwrap_or_default().len());
    raw.push_str(username);
    raw.push(':');
    raw.push_str(password.unwrap_or_default());
    let encoded = base64::engine::general_purpose::STANDARD.encode(raw.as_bytes());

    let mut value = HeaderValue::from_str(&format!("Basic {encoded}"))
        .map_err(|e| SignError::with_cause("basic credential is not a header value", e))?;
    value.set_sensitive(true);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_matches_the_rfc_encoding() {
        let value = basic_auth_value("account-1234", Some("licence-key")).unwrap();
        assert_eq!(
            value.to_str().unwrap(),
            "Basic YWNjb3VudC0xMjM0OmxpY2VuY2Uta2V5"
        );
    }

    #[test]
    fn a_basic_credential_is_marked_sensitive() {
        let value = basic_auth_value("user", Some("hunter2")).unwrap();
        assert!(value.is_sensitive());
        assert!(!format!("{value:?}").contains("hunter2"));
    }

    #[test]
    fn a_colon_in_the_basic_username_is_refused() {
        let error = basic_auth_value("account:1234", Some("hunter2"))
            .expect_err("the colon would move the field separator");
        assert!(error.to_string().contains("colon"), "{error}");
        assert!(!error.to_string().contains("hunter2"), "{error}");
    }

    #[test]
    fn a_retryable_signing_failure_says_so() {
        assert!(!SignError::new("no cause").is_retryable());
        assert!(SignError::new("idp unreachable").retryable().is_retryable());
    }

    /// A signer held behind an `Arc` or a reference signs as itself, so a
    /// shared signer needs no wrapper type to be passed per call.
    #[tokio::test]
    async fn a_shared_or_borrowed_signer_signs_as_itself() {
        struct Stamp;
        impl RequestSigner for Stamp {
            async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
                request
                    .headers_mut()
                    .insert("x-stamp", HeaderValue::from_static("1"));
                Ok(())
            }
        }
        let shared = std::sync::Arc::new(Stamp);
        let borrowed = &Stamp;
        let client = reqwest::Client::new();

        let mut request = client.get("https://api.example/").build().unwrap();
        shared.sign(&mut request).await.unwrap();
        assert_eq!(request.headers().get("x-stamp").unwrap(), "1");

        let mut request = client.get("https://api.example/").build().unwrap();
        borrowed.sign(&mut request).await.unwrap();
        assert_eq!(request.headers().get("x-stamp").unwrap(), "1");

        let mut request = client.get("https://api.example/").build().unwrap();
        (shared, borrowed).sign(&mut request).await.unwrap();
        assert_eq!(request.headers().get("x-stamp").unwrap(), "1");
    }

    #[test]
    fn sign_error_keeps_its_cause() {
        let cause = "not a header".parse::<u32>().unwrap_err();
        let error = SignError::with_cause("stamp failed", cause);
        assert_eq!(error.to_string(), "stamp failed");
        assert!(std::error::Error::source(&error).is_some());
        assert!(std::error::Error::source(&SignError::new("no cause")).is_none());
    }
}
