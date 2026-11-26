// Project:   scalo
// File:      src/wasm/trust.rs
// Purpose:   Who may sign a plugin, checked against its bundle or signature
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Who may sign a plugin, checked against its Sigstore bundle or its
//! detached signature.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sigstore_verify::crypto::VerificationKey;
use sigstore_verify::trust_root::{SIGSTORE_PRODUCTION_TRUSTED_ROOT, TrustedRoot};
use sigstore_verify::types::{Bundle, DerPublicKey, SignatureBytes};
use sigstore_verify::{Error as VerifyError, VerificationPolicy};

/// What a plugin must prove before it loads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct TrustPolicy {
    /// Load a plugin that has neither a bundle nor a signature, warning on
    /// every load. One that has either is still checked. Default `false`.
    pub allow_unsigned: bool,

    /// The sha256 the `.wasm` must have, as 64 hex digits.
    pub sha256: Option<String>,

    /// Who may sign a plugin. A plugin no signer here checks out against is
    /// refused.
    pub signers: Vec<Signer>,
}

/// One party trusted to sign plugins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Signer {
    /// A keyless Sigstore bundle, `name.wasm.sigstore.json`: a certificate
    /// naming `identity`, issued on a token from `issuer`, and its
    /// transparency log entry. A CI workflow signs this way.
    Keyless {
        /// The certificate's subject, such as a workflow URL or an email.
        identity: String,
        /// The OIDC issuer, such as `https://token.actions.githubusercontent.com`.
        issuer: String,
    },
    /// An operator's own key and a detached signature, `name.wasm.sig`: the
    /// base64 a `cosign sign-blob --key` writes. P-256 or Ed25519. Signing
    /// needs no network.
    Key {
        /// The public key, PEM.
        public_key: String,
    },
}

impl Signer {
    /// The `signer` label a plugin loaded under this signer reports.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Keyless { identity, .. } => identity.clone(),
            Self::Key { public_key } => DerPublicKey::from_pem(public_key).map_or_else(
                |_| "key:unreadable".to_string(),
                |key| {
                    let digest: [u8; 32] = Sha256::digest(key.as_bytes()).into();
                    format!("key:{}", &super::engine::hex(&digest)[..16])
                },
            ),
        }
    }
}

/// Why no signer accepted a plugin.
#[derive(Debug)]
pub(super) enum Refusal {
    /// The bundle or signature doesn't parse, or the bundle doesn't cover
    /// these bytes.
    Bad(String),
    /// No signer the policy names made it. For a key signature this includes
    /// bytes changed since signing, which a signature can't tell apart.
    Untrusted(String),
}

impl From<VerifyError> for Refusal {
    fn from(error: VerifyError) -> Self {
        match error {
            VerifyError::IdentityMismatch { .. }
            | VerifyError::IssuerMismatch { .. }
            | VerifyError::SignatureInvalid(_) => Self::Untrusted(error.to_string()),
            other => Self::Bad(other.to_string()),
        }
    }
}

/// What a plugin carries to prove who signed it.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Evidence<'a> {
    pub(super) bundle: Option<&'a [u8]>,
    pub(super) signature: Option<&'a [u8]>,
}

impl Evidence<'_> {
    pub(super) fn is_empty(&self) -> bool {
        self.bundle.is_none() && self.signature.is_none()
    }
}

/// The label of the first signer in `policy` that `evidence` checks out
/// against for `bytes`. A bundle is checked against the keyless signers, and
/// a signature against the keys.
pub(super) fn check(
    bytes: &[u8],
    evidence: Evidence<'_>,
    policy: &TrustPolicy,
    root: &TrustedRoot,
) -> Result<String, Refusal> {
    match (evidence.bundle, evidence.signature) {
        (Some(bundle), _) => check_bundle(bytes, bundle, policy, root),
        (None, Some(signature)) => check_signature(bytes, signature, policy),
        (None, None) => Err(Refusal::Untrusted("nothing signs it".to_string())),
    }
}

fn check_bundle(
    bytes: &[u8],
    bundle_json: &[u8],
    policy: &TrustPolicy,
    root: &TrustedRoot,
) -> Result<String, Refusal> {
    let json = std::str::from_utf8(bundle_json)
        .map_err(|_| Refusal::Bad("the bundle is not UTF-8".to_string()))?;
    let bundle = Bundle::from_json(json).map_err(|e| Refusal::Bad(format!("bundle: {e}")))?;
    let mut refusal = Refusal::Untrusted("the policy names no keyless signer".to_string());
    for signer in &policy.signers {
        let Signer::Keyless { identity, issuer } = signer else {
            continue;
        };
        let policy = VerificationPolicy::new(identity.as_str(), issuer.as_str());
        match sigstore_verify::verify(bytes, &bundle, &policy, root).map_err(Refusal::from) {
            Ok(_) => return Ok(signer.label()),
            // A bundle that fails for one identity fails for all.
            Err(bad @ Refusal::Bad(_)) => return Err(bad),
            Err(untrusted) => refusal = untrusted,
        }
    }
    Err(refusal)
}

fn check_signature(
    bytes: &[u8],
    signature: &[u8],
    policy: &TrustPolicy,
) -> Result<String, Refusal> {
    let text = std::str::from_utf8(signature)
        .map_err(|_| Refusal::Bad("the signature is not base64 text".to_string()))?;
    let signature = SignatureBytes::from_base64(text.trim())
        .map_err(|e| Refusal::Bad(format!("signature: {e}")))?;
    let mut refusal = Refusal::Untrusted("the policy names no key signer".to_string());
    for signer in &policy.signers {
        let Signer::Key { public_key } = signer else {
            continue;
        };
        let verified = DerPublicKey::from_pem(public_key)
            .map_err(|e| format!("signer key does not parse: {e}"))
            .and_then(|key| VerificationKey::from_spki(&key).map_err(|e| e.to_string()))
            .and_then(|key| key.verify(bytes, &signature).map_err(|e| e.to_string()));
        match verified {
            Ok(()) => return Ok(signer.label()),
            Err(detail) => refusal = Refusal::Untrusted(detail),
        }
    }
    Err(refusal)
}

/// The trusted root at `path`, or the public-good root built in.
pub(super) fn trusted_root(path: Option<&std::path::Path>) -> Result<TrustedRoot, String> {
    match path {
        Some(path) => TrustedRoot::from_file(path).map_err(|e| format!("{}: {e}", path.display())),
        None => TrustedRoot::from_json(SIGSTORE_PRODUCTION_TRUSTED_ROOT)
            .map_err(|e| format!("built-in root: {e}")),
    }
}

/// `hex` as 32 bytes, or `None` when it isn't 64 hex digits.
pub(super) fn parse_sha256(hex: &str) -> Option<[u8; 32]> {
    let hex = hex.as_bytes();
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0_u8; 32];
    let (pairs, _) = hex.as_chunks::<2>();
    for (byte, [high, low]) in out.iter_mut().zip(pairs) {
        let digit = |c: u8| char::from(c).to_digit(16);
        *byte = u8::try_from(digit(*high)? * 16 + digit(*low)?).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_digest_parses_only_as_64_hex_digits() {
        let ok = "00ff".repeat(16);
        let cases: [(&str, &str, bool); 5] = [
            ("64 hex digits", &ok, true),
            ("upper case", &ok.to_uppercase(), true),
            ("too short", "00ff", false),
            ("a non-hex digit", &format!("{}zz", &ok[..62]), false),
            ("empty", "", false),
        ];
        for (case, hex, parses) in cases {
            assert_eq!(parse_sha256(hex).is_some(), parses, "{case}");
        }
        assert_eq!(parse_sha256(&ok).unwrap()[..2], [0x00, 0xff]);
    }

    #[test]
    fn the_built_in_root_loads() {
        assert!(trusted_root(None).is_ok());
    }

    #[test]
    fn an_identity_or_signature_failure_is_untrusted_and_the_rest_is_bad() {
        let cases: [(&str, VerifyError, bool); 4] = [
            (
                "identity",
                VerifyError::IdentityMismatch {
                    expected: "a".into(),
                    actual: None,
                },
                true,
            ),
            (
                "issuer",
                VerifyError::IssuerMismatch {
                    expected: "a".to_string(),
                    actual: None,
                },
                true,
            ),
            (
                "signature",
                VerifyError::SignatureInvalid(String::new()),
                true,
            ),
            (
                "artifact",
                VerifyError::ArtifactMismatch(String::new()),
                false,
            ),
        ];
        for (case, error, untrusted) in cases {
            assert_eq!(
                matches!(Refusal::from(error), Refusal::Untrusted(_)),
                untrusted,
                "{case}"
            );
        }
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn a_policy_reads_from_yaml() {
        let yaml = "
allow_unsigned: false
sha256: 00ff
signers:
  - kind: keyless
    identity: https://github.com/acme/plugins/.github/workflows/release.yml@refs/heads/main
    issuer: https://token.actions.githubusercontent.com
  - kind: key
    public_key: PEM
";
        let policy: TrustPolicy = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(policy.signers.len(), 2);
        assert!(matches!(policy.signers[1], Signer::Key { .. }));
    }
}
