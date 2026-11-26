// Project:   scalo
// File:      src/tls/pin.rs
// Purpose:   The client verifier wrapper: key pins, and the TLS 1.2 warning and count
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The verifier every client config ends in: the trust choice's verifier,
//! then the key pins once the chain verifies.
//!
//! rustls calls `verify_tls12_signature` only in a TLS 1.2 handshake, so the
//! wrapper is also where a client sees one, whatever library drives the
//! connection.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::server::ParsedCertificate;
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};

use super::failure::PinMismatch;
use crate::crypto::CryptoProfile;
use crate::tls_config::SpkiPin;

/// The pin of the key in `cert`.
fn pin_of(cert: &CertificateDer<'_>) -> Result<SpkiPin, rustls::Error> {
    let spki = ParsedCertificate::try_from(cert)?.subject_public_key_info();
    Ok(SpkiPin(Sha256::digest(spki.as_ref()).into()))
}

/// `inner` with the server's key checked against `pins` once `inner` accepts
/// its chain, and each TLS 1.2 handshake on `profile` warned and counted.
pub(super) fn checked(
    inner: Arc<dyn ServerCertVerifier>,
    pins: &[SpkiPin],
    profile: CryptoProfile,
) -> Arc<dyn ServerCertVerifier> {
    Arc::new(CheckedVerifier {
        inner,
        pins: pins.to_vec(),
        profile,
    })
}

/// How often the TLS 1.2 warning repeats for one role and profile. A client
/// that reconnects per send would otherwise log one line per batch.
const TLS12_WARN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

/// Count one TLS 1.2 handshake on `profile`, from the side `role`, and warn
/// at most once per [`TLS12_WARN_INTERVAL`] for that role and profile.
pub(super) fn note_tls12(role: &'static str, profile: CryptoProfile) {
    if tls12_warn_due(role, profile, std::time::Instant::now()) {
        tracing::warn!(
            role,
            profile = profile.as_str(),
            "TLS 1.2 negotiated: the peer has no TLS 1.3, and allow_tls12 is on for this \
             profile. Repeats are counted in tls_tls12_handshakes_total and logged once an hour"
        );
    }
    #[cfg(feature = "metrics-core")]
    metrics::counter!(
        "tls_tls12_handshakes_total",
        "role" => role,
        "profile" => profile.as_str()
    )
    .increment(1);
}

/// Whether the TLS 1.2 warning for `role` and `profile` is due at `now`,
/// recording `now` as its last time when it is.
fn tls12_warn_due(role: &'static str, profile: CryptoProfile, now: std::time::Instant) -> bool {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static LAST: OnceLock<Mutex<HashMap<(&'static str, &'static str), std::time::Instant>>> =
        OnceLock::new();
    let mut last = LAST
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let key = (role, profile.as_str());
    match last.get(&key) {
        Some(at) if now.saturating_duration_since(*at) < TLS12_WARN_INTERVAL => false,
        _ => {
            last.insert(key, now);
            true
        }
    }
}

/// A verifier that accepts a chain only when `inner` does and, with pins set,
/// the leaf's key matches one of them.
#[derive(Debug)]
struct CheckedVerifier {
    inner: Arc<dyn ServerCertVerifier>,
    pins: Vec<SpkiPin>,
    profile: CryptoProfile,
}

impl ServerCertVerifier for CheckedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let verified = self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        if self.pins.is_empty() {
            return Ok(verified);
        }
        let observed = pin_of(end_entity)?;
        if self.pins.contains(&observed) {
            return Ok(verified);
        }
        tracing::warn!(
            server = %server_name.to_str(),
            observed = %observed,
            pins = self.pins.len(),
            "TLS: the server's public key matches no configured pin"
        );
        Err(rustls::Error::InvalidCertificate(CertificateError::Other(
            rustls::OtherError(Arc::new(PinMismatch)),
        )))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let valid = self.inner.verify_tls12_signature(message, cert, dss)?;
        note_tls12("client", self.profile);
        Ok(valid)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.inner.requires_raw_public_keys()
    }

    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        self.inner.root_hint_subjects()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The warning goes once per role and profile until the interval passes.
    /// Each case uses its own role so tests sharing the process don't collide.
    #[test]
    fn the_tls12_warning_repeats_once_an_interval_per_role_and_profile() {
        let start = std::time::Instant::now();
        let later = start + TLS12_WARN_INTERVAL;
        let cases: [(&str, &'static str, CryptoProfile, std::time::Instant, bool); 5] = [
            ("first", "warn-a", CryptoProfile::Commercial, start, true),
            (
                "same key, same instant",
                "warn-a",
                CryptoProfile::Commercial,
                start,
                false,
            ),
            (
                "another profile",
                "warn-a",
                CryptoProfile::Strong,
                start,
                true,
            ),
            (
                "another role",
                "warn-b",
                CryptoProfile::Commercial,
                start,
                true,
            ),
            (
                "a full interval later",
                "warn-a",
                CryptoProfile::Commercial,
                later,
                true,
            ),
        ];
        for (name, role, profile, now, want) in cases {
            assert_eq!(tls12_warn_due(role, profile, now), want, "{name}");
        }
    }
}
