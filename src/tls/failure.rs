// Project:   scalo
// File:      src/tls/failure.rs
// Purpose:   Typed reasons a TLS handshake failed, read from any error chain
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`TlsFailure`] read from rustls: why a handshake failed, in words that
//! name the fix.
//!
//! The reasons come from the rustls error or alert in an error chain, so they
//! are found the same way under reqwest, tonic and a raw stream. A client
//! never sees the server's offer, only the alert it sent, so a refusal of
//! every suite and group is worded from that alert.

use super::profile::Posture;
use crate::tls_config::{TlsFailure, Whose};

/// The error the key pin check fails a chain with, found by [`TlsFailure`].
#[derive(Debug, thiserror::Error)]
#[error("the server's public key matches no configured pin")]
pub(crate) struct PinMismatch;

impl TlsFailure {
    /// The failure behind the rustls error in `error` or its causes, for a
    /// connection on `posture`. `None` when there is none, or the error is
    /// not one of these.
    #[must_use]
    pub fn find(error: &(dyn std::error::Error + 'static), posture: Posture) -> Option<Self> {
        super::rustls_error(error).and_then(|tls| Self::of(tls, posture))
    }

    /// The failure `error` is, for a connection on `posture`.
    #[must_use]
    pub fn of(error: &rustls::Error, posture: Posture) -> Option<Self> {
        use rustls::{AlertDescription as A, CertificateError as C, PeerIncompatible as P};

        let profile = posture.profile;
        let no_shared = || Self::NoSharedParameters {
            profile,
            wider: profile.wider(),
        };
        let version = || Self::VersionMismatch {
            profile,
            tls12_offered: posture.allow_tls12,
        };
        Some(match error {
            rustls::Error::InvalidCertificate(certificate) => match certificate {
                C::NotValidForName | C::NotValidForNameContext { .. } => Self::NameMismatch,
                C::Expired | C::ExpiredContext { .. } => Self::Expired { whose: Whose::Peer },
                C::NotValidYet | C::NotValidYetContext { .. } => {
                    Self::NotYetValid { whose: Whose::Peer }
                }
                C::Other(other) if other.0.downcast_ref::<PinMismatch>().is_some() => {
                    Self::PinMismatch
                }
                _ => Self::UntrustedChain,
            },
            rustls::Error::AlertReceived(alert) => match alert {
                A::ProtocolVersion => version(),
                A::HandshakeFailure | A::InsufficientSecurity => no_shared(),
                A::CertificateExpired => Self::Expired { whose: Whose::Ours },
                A::BadCertificate
                | A::UnknownCA
                | A::CertificateRequired
                | A::CertificateUnknown
                | A::CertificateRevoked
                | A::UnsupportedCertificate
                | A::AccessDenied => Self::ClientCertRejected,
                _ => return None,
            },
            rustls::Error::PeerIncompatible(incompatible) => match incompatible {
                P::ServerDoesNotSupportTls12Or13
                | P::ServerTlsVersionIsDisabledByOurConfig
                | P::SupportedVersionsExtensionRequired
                | P::Tls12NotOffered
                | P::Tls12NotOfferedOrEnabled => version(),
                P::NoCipherSuitesInCommon
                | P::NoKxGroupsInCommon
                | P::NoSignatureSchemesInCommon
                | P::NoCertificateRequestSignatureSchemesInCommon => no_shared(),
                _ => return None,
            },
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Each rustls error and alert maps to its reason, found bare and inside
    /// an `io::Error`, and the hints name the fix.
    #[test]
    fn rustls_errors_map_to_their_reason() {
        use rustls::{AlertDescription as A, CertificateError as C, Error as E};

        use crate::crypto::{CryptoProfile, KeyShare, TlsProfiles};

        let posture = |profile| Posture::new(profile, &TlsProfiles::default(), KeyShare::Profile);
        let strong = CryptoProfile::Strong;
        let cases: Vec<(E, Option<TlsFailure>)> = vec![
            (
                E::AlertReceived(A::ProtocolVersion),
                Some(TlsFailure::VersionMismatch {
                    profile: strong,
                    tls12_offered: false,
                }),
            ),
            (
                E::AlertReceived(A::HandshakeFailure),
                Some(TlsFailure::NoSharedParameters {
                    profile: strong,
                    wider: Some(CryptoProfile::Commercial),
                }),
            ),
            (
                E::InvalidCertificate(C::UnknownIssuer),
                Some(TlsFailure::UntrustedChain),
            ),
            (
                E::InvalidCertificate(C::NotValidForName),
                Some(TlsFailure::NameMismatch),
            ),
            (
                E::InvalidCertificate(C::Expired),
                Some(TlsFailure::Expired { whose: Whose::Peer }),
            ),
            (
                E::AlertReceived(A::CertificateExpired),
                Some(TlsFailure::Expired { whose: Whose::Ours }),
            ),
            (
                E::InvalidCertificate(C::NotValidYet),
                Some(TlsFailure::NotYetValid { whose: Whose::Peer }),
            ),
            (
                E::InvalidCertificate(C::Other(rustls::OtherError(Arc::new(PinMismatch)))),
                Some(TlsFailure::PinMismatch),
            ),
            (
                E::AlertReceived(A::CertificateRequired),
                Some(TlsFailure::ClientCertRejected),
            ),
            (E::AlertReceived(A::CloseNotify), None),
            (E::DecryptError, None),
        ];
        for (error, want) in cases {
            let name = format!("{error:?}");
            assert_eq!(TlsFailure::of(&error, posture(strong)), want, "{name}");
            let wrapped = std::io::Error::new(std::io::ErrorKind::InvalidData, error);
            assert_eq!(
                TlsFailure::find(&wrapped, posture(strong)),
                want,
                "{name} in an io::Error"
            );
        }

        let message = |error: E, profile| {
            TlsFailure::of(&error, posture(profile))
                .unwrap()
                .to_string()
        };
        let cases = [
            (
                E::AlertReceived(A::ProtocolVersion),
                strong,
                "peer supports only TLS 1.2: enable allow_tls12 on profile strong",
            ),
            (
                E::AlertReceived(A::ProtocolVersion),
                CryptoProfile::Commercial,
                "the peer has neither TLS 1.3 nor 1.2, which profile commercial offers",
            ),
            (
                E::AlertReceived(A::ProtocolVersion),
                CryptoProfile::Cnsa2,
                "the peer has no TLS 1.3, and profile cnsa2 has no TLS 1.2",
            ),
            (
                E::AlertReceived(A::HandshakeFailure),
                CryptoProfile::Cnsa2,
                "the peer refused every suite and group profile `cnsa2` offers: try profile `strong`",
            ),
        ];
        for (error, profile, want) in cases {
            assert_eq!(message(error, profile), want);
        }
    }
}
