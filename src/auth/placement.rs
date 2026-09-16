// Project:   scalo
// File:      src/auth/placement.rs
// Purpose:   Where a credential goes on the request
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Where the credential goes: a header, a query parameter, basic auth.
//!
//! Each placement is a [`RequestSigner`] over a [`CredentialSource`], generic
//! over the source so a cache hit is a pointer clone rather than a virtual call
//! and an allocation. Two placements over one `Arc` source are two headers and
//! one exchange -- compose them as a tuple, which is a signer itself.

use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};

use super::source::CredentialSource;
use crate::http_client::signer::basic_auth_value;
use crate::http_client::{RequestSigner, SignError};

/// Put the credential in a header, after an optional prefix.
#[derive(Debug, Clone)]
pub struct HeaderPlacement<S> {
    name: HeaderName,
    prefix: Box<str>,
    source: S,
}

impl<S> HeaderPlacement<S> {
    /// Write `<prefix><secret>` into `name`. The prefix is empty for the
    /// providers that want the bare key (`DD-API-KEY`, `X-API-Key`).
    #[must_use]
    pub fn new(name: HeaderName, prefix: impl Into<Box<str>>, source: S) -> Self {
        Self {
            name,
            prefix: prefix.into(),
            source,
        }
    }

    /// `Authorization: Bearer <secret>`, the OAuth2 case.
    #[must_use]
    pub fn bearer(source: S) -> Self {
        Self::new(AUTHORIZATION, "Bearer ", source)
    }
}

impl<S: CredentialSource> RequestSigner for HeaderPlacement<S> {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        let credential = self.source.credential().await?;
        let rendered = format!("{}{}", self.prefix, credential.secret.expose());
        let mut value = HeaderValue::from_str(&rendered)
            .map_err(|e| SignError::with_cause("credential is not a header value", e))?;
        // So the http crate's own Debug renders it as Sensitive, not as the
        // credential, wherever a request or its headers are formatted.
        value.set_sensitive(true);
        request.headers_mut().insert(self.name.clone(), value);
        Ok(())
    }
}

/// Put the credential in a query parameter.
///
/// It goes on the built URL rather than being formatted into the URL string, so
/// the URL a caller logs before the call never carries it.
#[derive(Debug, Clone)]
pub struct QueryPlacement<S> {
    name: Box<str>,
    source: S,
}

impl<S> QueryPlacement<S> {
    /// Append `name=<secret>` to the query.
    #[must_use]
    pub fn new(name: impl Into<Box<str>>, source: S) -> Self {
        Self {
            name: name.into(),
            source,
        }
    }
}

impl<S: CredentialSource> RequestSigner for QueryPlacement<S> {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        let credential = self.source.credential().await?;
        request
            .url_mut()
            .query_pairs_mut()
            .append_pair(&self.name, credential.secret.expose());
        Ok(())
    }
}

/// Put the credential in HTTP basic auth as the password, under a fixed
/// username (an account id, a licence holder).
#[derive(Debug, Clone)]
pub struct BasicPlacement<S> {
    username: Box<str>,
    source: S,
}

impl<S> BasicPlacement<S> {
    /// Authenticate as `username`, with the credential as the password.
    #[must_use]
    pub fn new(username: impl Into<Box<str>>, source: S) -> Self {
        Self {
            username: username.into(),
            source,
        }
    }
}

impl<S: CredentialSource> RequestSigner for BasicPlacement<S> {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        let credential = self.source.credential().await?;
        let value = basic_auth_value(&self.username, Some(credential.secret.expose()))?;
        request.headers_mut().insert(AUTHORIZATION, value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::source::{Cached, Static};
    use super::*;

    fn request(url: &str) -> reqwest::Request {
        reqwest::Request::new(reqwest::Method::GET, url.parse().unwrap())
    }

    #[tokio::test]
    async fn a_bearer_header_is_prefixed_and_sensitive() {
        let placement = HeaderPlacement::bearer(Cached::new(Static::new("tok")));
        let mut request = request("https://api.example/things");

        placement.sign(&mut request).await.unwrap();

        let value = request.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(value.to_str().unwrap(), "Bearer tok");
        assert!(value.is_sensitive());
        assert!(!format!("{:?}", request.headers()).contains("tok"));
    }

    #[tokio::test]
    async fn a_query_placement_keeps_an_existing_parameter() {
        let placement = QueryPlacement::new("token", Cached::new(Static::new("tok")));
        let mut request = request("https://api.example/things?page=2");

        placement.sign(&mut request).await.unwrap();

        assert_eq!(request.url().query(), Some("page=2&token=tok"));
    }

    /// Two placements over one source is the shape a provider wanting two
    /// headers needs: a tuple is a signer, so no new type and no `dyn`.
    #[tokio::test]
    async fn a_tuple_of_placements_applies_both() {
        let source = Arc::new(Cached::new(Static::new("tok")));
        let signer = (
            HeaderPlacement::new(
                HeaderName::from_static("dd-api-key"),
                "",
                Arc::clone(&source),
            ),
            HeaderPlacement::new(
                HeaderName::from_static("dd-application-key"),
                "",
                Arc::clone(&source),
            ),
        );
        let mut request = request("https://api.example/things");

        signer.sign(&mut request).await.unwrap();

        assert_eq!(request.headers().get("dd-api-key").unwrap(), "tok");
        assert_eq!(request.headers().get("dd-application-key").unwrap(), "tok");
    }
}
