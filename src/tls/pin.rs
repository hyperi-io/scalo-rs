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

/// Log and count one TLS 1.2 handshake on `profile`, from the side `role`.
pub(super) fn note_tls12(role: &'static str, profile: CryptoProfile) {
    tracing::warn!(
        role,
        profile = profile.as_str(),
        "TLS 1.2 negotiated: the peer has no TLS 1.3, and allow_tls12 is on for this profile"
    );
    #[cfg(feature = "metrics-core")]
    metrics::counter!(
        "tls_tls12_handshakes_total",
        "role" => role,
        "profile" => profile.as_str()
    )
    .increment(1);
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
