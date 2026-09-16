// Project:   scalo
// File:      src/auth/error.rs
// Purpose:   Credential acquisition failures
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! What can go wrong acquiring a credential.

use crate::http_client::{HttpError, SignError};

/// Why a credential could not be acquired.
///
/// No variant carries the credential: the message a consumer logs names the
/// endpoint and what it said, never what was sent to it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuthError {
    /// The endpoint could not be reached, or the transport failed.
    #[error("credential exchange at {url} failed: {source}")]
    Unreachable {
        /// The endpoint that was called.
        url: String,
        /// The transport failure, with the request URL stripped out of it.
        #[source]
        source: HttpError,
    },

    /// The endpoint answered, and refused.
    #[error("credential exchange at {url} refused with status {status}: {body}")]
    Refused {
        /// The endpoint that was called.
        url: String,
        /// The status it answered with.
        status: u16,
        /// What it said, truncated -- providers name the reason in the body.
        body: String,
    },

    /// The endpoint answered 2xx with something that is not a credential.
    #[error("credential response from {url} is not usable: {reason}")]
    Malformed {
        /// The endpoint that was called.
        url: String,
        /// What was wrong with the response.
        reason: String,
    },
}

/// A placement reports an acquisition failure as a signing failure: the request
/// it was asked to sign is the thing that cannot proceed.
impl From<AuthError> for SignError {
    fn from(error: AuthError) -> Self {
        Self::with_cause("credential unavailable", error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_names_the_endpoint_and_the_status() {
        let error = AuthError::Refused {
            url: "https://idp.example/token".to_owned(),
            status: 400,
            body: "{\"error\":\"invalid_scope\"}".to_owned(),
        };
        let rendered = error.to_string();
        assert!(rendered.contains("https://idp.example/token"), "{rendered}");
        assert!(rendered.contains("400"), "{rendered}");
        assert!(rendered.contains("invalid_scope"), "{rendered}");
    }

    #[test]
    fn a_signing_failure_keeps_the_acquisition_failure_as_its_cause() {
        let sign: SignError = AuthError::Malformed {
            url: "https://idp.example/token".to_owned(),
            reason: "no access_token".to_owned(),
        }
        .into();
        assert_eq!(sign.to_string(), "credential unavailable");
        let cause = std::error::Error::source(&sign).expect("the cause is kept");
        assert!(cause.to_string().contains("no access_token"));
    }
}
