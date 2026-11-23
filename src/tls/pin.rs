// Project:   scalo
// File:      src/tls/pin.rs
// Purpose:   SHA-256 pins on a server's public key, checked after its chain
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Public key pins: the SHA-256 of a server certificate's SubjectPublicKeyInfo,
//! checked once the certificate chain has verified.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::server::ParsedCertificate;
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::TlsError;

/// The SHA-256 of a certificate's SubjectPublicKeyInfo: a pin on the key, so a
/// certificate re-issued on the same key keeps it.
///
/// Written in base64 or as 64 hex digits. `docs/api/tls.md` has the openssl
/// line that prints a certificate's pin.
///
/// ```rust
/// let pin: scalo::tls::SpkiPin = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".parse()?;
/// assert_eq!(pin.to_string(), "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
/// # Ok::<(), scalo::tls::TlsError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpkiPin([u8; 32]);

impl SpkiPin {
    /// The pin of the key in `cert`.
    fn of(cert: &CertificateDer<'_>) -> Result<Self, rustls::Error> {
        let spki = ParsedCertificate::try_from(cert)?.subject_public_key_info();
        Ok(Self(Sha256::digest(spki.as_ref()).into()))
    }
}

impl FromStr for SpkiPin {
    type Err = TlsError;

    fn from_str(text: &str) -> Result<Self, TlsError> {
        let bytes: Option<Vec<u8>> =
            if text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit()) {
                text.as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| {
                        std::str::from_utf8(pair)
                            .ok()
                            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                    })
                    .collect()
            } else {
                STANDARD.decode(text).ok()
            };
        bytes
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .map(Self)
            .ok_or_else(|| TlsError::InvalidPin {
                pin: text.to_string(),
            })
    }
}

impl fmt::Display for SpkiPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&STANDARD.encode(self.0))
    }
}

impl fmt::Debug for SpkiPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SpkiPin").field(&self.to_string()).finish()
    }
}

impl Serialize for SpkiPin {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SpkiPin {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "config-schema")]
impl schemars::JsonSchema for SpkiPin {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SpkiPin".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^([0-9A-Fa-f]{64}|[A-Za-z0-9+/]{43}=)$",
            "description": "SHA-256 of a server certificate's SubjectPublicKeyInfo, in base64 or 64 hex digits."
        })
    }
}

/// `inner`, with the server's key checked against `pins` once `inner` accepts
/// its chain. No pins returns `inner` itself.
pub(super) fn pinned(
    inner: Arc<dyn ServerCertVerifier>,
    pins: &[SpkiPin],
) -> Arc<dyn ServerCertVerifier> {
    if pins.is_empty() {
        return inner;
    }
    Arc::new(PinnedVerifier {
        inner,
        pins: pins.to_vec(),
    })
}

/// A verifier that accepts a chain only when `inner` does and the leaf's key
/// matches one of `pins`.
#[derive(Debug)]
struct PinnedVerifier {
    inner: Arc<dyn ServerCertVerifier>,
    pins: Vec<SpkiPin>,
}

impl ServerCertVerifier for PinnedVerifier {
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
        let observed = SpkiPin::of(end_entity)?;
        if self.pins.contains(&observed) {
            return Ok(verified);
        }
        tracing::warn!(
            server = %server_name.to_str(),
            observed = %observed,
            pins = self.pins.len(),
            "TLS: the server's public key matches no configured pin"
        );
        Err(rustls::Error::InvalidCertificate(
            CertificateError::ApplicationVerificationFailure,
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
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
    use serde::de::value::{Error, StrDeserializer};

    use super::*;

    /// Base64 and hex of the same 32 bytes read as one pin, which prints as
    /// base64. Any other text fails to parse, from text and from config.
    #[test]
    fn a_pin_reads_base64_or_hex_and_nothing_else() {
        let base64 = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        let hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let from_base64: SpkiPin = base64.parse().unwrap();
        for text in [hex, hex.to_uppercase().as_str()] {
            assert_eq!(text.parse::<SpkiPin>().unwrap(), from_base64, "{text}");
        }
        assert_eq!(from_base64.to_string(), base64);

        let bad = [
            ("empty", String::new()),
            (
                "base64 without its padding",
                base64.trim_end_matches('=').to_string(),
            ),
            ("base64 of 31 bytes", STANDARD.encode([0u8; 31])),
            // 48 zero bytes are 64 `A`s, which read as hex, so use 0xff.
            (
                "base64 of 48 bytes, a SHA-384",
                STANDARD.encode([0xffu8; 48]),
            ),
            ("63 hex digits", hex[1..].to_string()),
            ("64 characters, one not hex", format!("{}g", &hex[1..])),
            ("hex with a leading plus", format!("+{}", &hex[1..])),
            ("a pin with spaces around it", format!(" {base64} ")),
        ];
        for (name, text) in bad {
            let error = text.parse::<SpkiPin>().unwrap_err().to_string();
            assert!(
                error.contains("is not a SHA-256 in base64 or 64 hex digits"),
                "{name}: {error}"
            );
            let from_config = SpkiPin::deserialize(StrDeserializer::<Error>::new(&text));
            assert!(from_config.is_err(), "{name} must not load from config");
        }
    }
}
