// Project:   scalo
// File:      src/auth/error.rs
// Purpose:   Credential acquisition failures
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! What can go wrong acquiring a credential.

use std::fmt::Write as _;

use crate::http_client::{HttpClientError, HttpError};

/// Why a credential could not be acquired.
///
/// No variant carries the credential: the message a consumer logs names the
/// endpoint and what it said, never what was sent to it. The endpoint is named
/// as scheme, host and path, because a query string or a userinfo section
/// carries credentials as readily as the form does.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuthError {
    /// The endpoint could not be reached, or the transport failed.
    #[error("credential exchange at {url} failed: {source}")]
    Unreachable {
        /// The endpoint that was called, as scheme, host and path.
        url: String,
        /// The transport failure, with the request URL stripped out of it.
        /// Boxed because that failure can itself carry a signing failure
        /// carrying one of these.
        #[source]
        source: Box<HttpError>,
    },

    /// The endpoint answered, and refused.
    #[error("credential exchange at {url} refused with status {status}: {detail}")]
    Refused {
        /// The endpoint that was called, as scheme, host and path.
        url: String,
        /// The status it answered with.
        status: u16,
        /// The error fields of the refusal, never the body it sent.
        detail: String,
    },

    /// The endpoint answered 2xx with something that is not a credential.
    #[error("credential response from {url} is not usable: {reason}")]
    Malformed {
        /// The endpoint that was called, as scheme, host and path.
        url: String,
        /// What was wrong with the response.
        reason: String,
    },

    /// The endpoint a source was built with cannot be used at all.
    #[error("token endpoint {url} is not usable: {reason}")]
    Endpoint {
        /// The endpoint as given, as scheme, host and path.
        url: String,
        /// Why it was refused.
        reason: String,
    },

    /// The exchange did not finish inside its own deadline.
    #[error("credential exchange did not answer within {secs}s")]
    TimedOut {
        /// The deadline that passed.
        secs: u64,
    },

    /// The acquisition this caller waited on failed. One in-flight acquisition
    /// is shared with every waiter, so they report one failure rather than each
    /// hitting an endpoint that has just refused.
    #[error("{message}")]
    Shared {
        /// What the one acquisition reported.
        message: String,
        /// Whether that failure was one another attempt could get past: the
        /// endpoint unreachable or out of time, rather than a refusal.
        transient: bool,
    },

    /// The exchange's own HTTP client could not be built.
    #[error("credential exchange client could not be built: {source}")]
    Client {
        /// The build failure.
        #[source]
        source: HttpClientError,
    },

    /// The consumer could not supply the credential at all: a secret spec that
    /// did not resolve, a key file that is not there. Nothing to exchange, and
    /// nothing another attempt would change.
    #[error("not supplied by the consumer: {reason}")]
    Unavailable {
        /// Why, without the spec's secret text or the value it named.
        reason: String,
    },
}

/// A placement reports an acquisition failure as
/// [`SignError::Auth`](crate::http_client::SignError::Auth), whole: the request
/// it was asked to sign is the thing that cannot proceed, and the consumer can
/// still read the status of a refusal off it. The retry loop re-signs when
/// [`AuthError::is_transient`] says another attempt could get past it.
impl AuthError {
    /// Whether another attempt could get past this failure: the endpoint was
    /// unreachable or out of time, or a waiter was handed such a failure. A
    /// refusal, a malformed response, an unusable endpoint and a credential the
    /// consumer could not supply are the same answer next time.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Unreachable { .. } | Self::TimedOut { .. } => true,
            Self::Shared { transient, .. } => *transient,
            Self::Refused { .. }
            | Self::Malformed { .. }
            | Self::Endpoint { .. }
            | Self::Client { .. }
            | Self::Unavailable { .. } => false,
        }
    }
}

/// The endpoint an error is allowed to name: scheme, host, port and path.
///
/// The query and any userinfo are dropped rather than trimmed to a length: a
/// provider that wants its credential in the URL puts it in either of them, and
/// the whole point of naming the endpoint is to say which host refused.
pub(crate) fn endpoint_name(url: &str) -> String {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return "an unparseable endpoint".to_owned();
    };
    let mut named = String::with_capacity(url.len());
    named.push_str(parsed.scheme());
    named.push_str("://");
    named.push_str(parsed.host_str().unwrap_or_default());
    if let Some(port) = parsed.port() {
        let _ = write!(named, ":{port}");
    }
    named.push_str(parsed.path());
    named
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::SignError;

    #[test]
    fn a_refusal_names_the_endpoint_and_the_status() {
        let error = AuthError::Refused {
            url: "https://idp.example/token".to_owned(),
            status: 400,
            detail: "error=invalid_scope".to_owned(),
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
        assert!(
            sign.to_string().starts_with("credential unavailable"),
            "{sign}"
        );
        let cause = std::error::Error::source(&sign).expect("the cause is kept");
        assert!(cause.to_string().contains("no access_token"));
    }

    #[test]
    fn a_refusal_reaches_the_signer_with_its_status() {
        let sign: SignError = AuthError::Refused {
            url: "https://idp.example/token".to_owned(),
            status: 401,
            detail: "error=invalid_client".to_owned(),
        }
        .into();
        assert!(
            matches!(
                sign,
                SignError::Auth(AuthError::Refused { status: 401, .. })
            ),
            "the status is there to match on, not boxed away: {sign:?}"
        );
    }

    #[test]
    fn a_credential_the_consumer_could_not_supply_says_why_and_nothing_else() {
        let error = AuthError::Unavailable {
            reason: "secret spec did not resolve".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            "not supplied by the consumer: secret spec did not resolve"
        );
        assert!(
            !error.is_transient(),
            "nothing another attempt would change"
        );
        assert!(!SignError::from(error).is_retryable());
    }

    #[test]
    fn only_a_transient_acquisition_failure_is_worth_signing_again() {
        let unreachable: SignError = AuthError::TimedOut { secs: 30 }.into();
        assert!(unreachable.is_retryable());

        let refused: SignError = AuthError::Refused {
            url: "https://idp.example/token".to_owned(),
            status: 401,
            detail: "error=invalid_client".to_owned(),
        }
        .into();
        assert!(
            !refused.is_retryable(),
            "a refusal is the same refusal next attempt"
        );

        let waited_on_a_timeout: SignError = AuthError::Shared {
            message: "credential exchange did not answer within 30s".to_owned(),
            transient: true,
        }
        .into();
        assert!(
            waited_on_a_timeout.is_retryable(),
            "a waiter retries the same way the caller that ran the exchange does"
        );
    }

    #[test]
    fn an_endpoint_is_named_without_its_query_or_userinfo() {
        assert_eq!(
            endpoint_name("https://user:pw@idp.example:8443/oauth2/token?wrapping=secret"),
            "https://idp.example:8443/oauth2/token"
        );
        assert_eq!(
            endpoint_name("https://idp.example/oauth2/token"),
            "https://idp.example/oauth2/token"
        );
        assert_eq!(endpoint_name("not a url"), "an unparseable endpoint");
    }
}
