// Project:   scalo
// File:      src/tls/profile.rs
// Purpose:   The rustls versions, suites, groups and signature schemes of each TLS profile
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Each [`CryptoProfile`] as a rustls provider over aws-lc-rs.
//!
//! Suites and groups are named from rustls's constants, never its defaults, so
//! a rustls release that changes its defaults changes no suite or group. cnsa2
//! names its signature schemes too. strong and commercial verify aws-lc-rs's
//! full signature set, which a rustls release can change.

use std::sync::{Arc, LazyLock};

use rustls::crypto::aws_lc_rs::{cipher_suite as cs, kx_group as kx};
use rustls::crypto::{CryptoProvider, SupportedKxGroup, WebPkiSupportedAlgorithms};
use rustls::pki_types::SignatureVerificationAlgorithm;
use rustls::pki_types::alg_id;
use rustls::{NamedGroup, SignatureScheme, SupportedCipherSuite, SupportedProtocolVersion};

use crate::crypto::{CryptoProfile, KeyShare, TlsProfiles};

/// A profile with its cascade settings applied: what one config is built on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileSettings {
    /// The profile.
    pub profile: CryptoProfile,
    /// Whether TLS 1.2 is offered beside TLS 1.3.
    pub allow_tls12: bool,
    /// The key share a client sends first.
    pub key_share: KeyShare,
}

impl ProfileSettings {
    /// `profile` with its `profiles` settings, sending `key_share` first.
    #[must_use]
    pub fn new(profile: CryptoProfile, profiles: &TlsProfiles, key_share: KeyShare) -> Self {
        Self {
            profile,
            allow_tls12: profile.has_tls12() && profiles.options(profile).allow_tls12,
            key_share,
        }
    }

    /// The protocol versions offered.
    #[must_use]
    pub fn versions(self) -> &'static [&'static SupportedProtocolVersion] {
        static TLS13: [&SupportedProtocolVersion; 1] = [&rustls::version::TLS13];
        static TLS13_AND_12: [&SupportedProtocolVersion; 2] =
            [&rustls::version::TLS13, &rustls::version::TLS12];
        if self.allow_tls12 {
            &TLS13_AND_12
        } else {
            &TLS13
        }
    }

    /// The cipher suites offered, most preferred first.
    #[must_use]
    pub fn cipher_suites(self) -> Vec<SupportedCipherSuite> {
        let tls13: &[SupportedCipherSuite] = match self.profile {
            // AES-128-GCM last: RFC 8446 section 9.1 makes it mandatory to implement.
            CryptoProfile::Strong => &[
                cs::TLS13_AES_256_GCM_SHA384,
                cs::TLS13_CHACHA20_POLY1305_SHA256,
                cs::TLS13_AES_128_GCM_SHA256,
            ],
            CryptoProfile::Commercial => &[
                cs::TLS13_AES_128_GCM_SHA256,
                cs::TLS13_AES_256_GCM_SHA384,
                cs::TLS13_CHACHA20_POLY1305_SHA256,
            ],
            CryptoProfile::Cnsa2 => &[cs::TLS13_AES_256_GCM_SHA384],
        };
        let tls12: &[SupportedCipherSuite] = match self.profile {
            CryptoProfile::Strong => &[
                cs::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                cs::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            ],
            CryptoProfile::Commercial => &[
                cs::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                cs::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
                cs::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
                cs::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                cs::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                cs::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
            ],
            CryptoProfile::Cnsa2 => &[],
        };
        let mut suites = tls13.to_vec();
        if self.allow_tls12 {
            suites.extend_from_slice(tls12);
        }
        suites
    }

    /// The key-exchange groups offered, the first one shared.
    #[must_use]
    pub fn kx_groups(self) -> Vec<&'static dyn SupportedKxGroup> {
        let mut groups: Vec<&'static dyn SupportedKxGroup> = match self.profile {
            CryptoProfile::Strong => vec![
                kx::X25519MLKEM768,
                kx::SECP256R1MLKEM768,
                kx::SECP384R1,
                kx::X25519,
                kx::SECP256R1,
            ],
            CryptoProfile::Commercial => {
                vec![kx::X25519MLKEM768, kx::X25519, kx::SECP256R1, kx::SECP384R1]
            }
            CryptoProfile::Cnsa2 => vec![kx::MLKEM1024, kx::SECP384R1],
        };
        if self.key_share == KeyShare::Classical {
            // Stable, so each half keeps the profile's order.
            groups.sort_by_key(|group| is_post_quantum(group.name()));
        }
        groups
    }

    /// The certificate and handshake signatures accepted.
    #[must_use]
    pub fn signatures(self) -> WebPkiSupportedAlgorithms {
        match self.profile {
            CryptoProfile::Strong | CryptoProfile::Commercial => {
                rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms
            }
            CryptoProfile::Cnsa2 => *CNSA2_SIGNATURES,
        }
    }

    /// The aws-lc-rs provider holding these settings' suites, groups and
    /// signatures.
    #[must_use]
    pub fn provider(self) -> Arc<CryptoProvider> {
        let mut provider = rustls::crypto::aws_lc_rs::default_provider();
        provider.cipher_suites = self.cipher_suites();
        provider.kx_groups = self.kx_groups();
        provider.signature_verification_algorithms = self.signatures();
        Arc::new(provider)
    }
}

/// Whether `group` carries an ML-KEM key exchange, alone or in a hybrid.
#[must_use]
pub fn is_post_quantum(group: NamedGroup) -> bool {
    matches!(
        group,
        NamedGroup::X25519MLKEM768
            | NamedGroup::secp256r1MLKEM768
            | NamedGroup::MLKEM768
            | NamedGroup::MLKEM1024
    )
}

/// One CNSA 2.0 signature scheme and the certificate algorithms it accepts.
struct Cnsa2Scheme {
    scheme: SignatureScheme,
    algorithms: LazyLock<Vec<&'static dyn SignatureVerificationAlgorithm>>,
}

/// The aws-lc-rs algorithms with `key` and `signature` identifiers.
fn algorithms(
    key: rustls::pki_types::AlgorithmIdentifier,
    signature: rustls::pki_types::AlgorithmIdentifier,
) -> Vec<&'static dyn SignatureVerificationAlgorithm> {
    rustls::crypto::aws_lc_rs::default_provider()
        .signature_verification_algorithms
        .all
        .iter()
        .copied()
        .filter(|alg| alg.public_key_alg_id() == key && alg.signature_alg_id() == signature)
        .collect()
}

/// ECDSA P-384 with SHA-384, and ML-DSA-87.
static CNSA2_SCHEMES: [Cnsa2Scheme; 2] = [
    Cnsa2Scheme {
        scheme: SignatureScheme::ECDSA_NISTP384_SHA384,
        algorithms: LazyLock::new(|| algorithms(alg_id::ECDSA_P384, alg_id::ECDSA_SHA384)),
    },
    Cnsa2Scheme {
        scheme: SignatureScheme::ML_DSA_87,
        algorithms: LazyLock::new(|| algorithms(alg_id::ML_DSA_87, alg_id::ML_DSA_87)),
    },
];

static CNSA2_ALL: LazyLock<Vec<&'static dyn SignatureVerificationAlgorithm>> =
    LazyLock::new(|| {
        CNSA2_SCHEMES
            .iter()
            .flat_map(|scheme| scheme.algorithms.iter().copied())
            .collect()
    });

type SchemeAlgorithms = (
    SignatureScheme,
    &'static [&'static dyn SignatureVerificationAlgorithm],
);

static CNSA2_MAPPING: LazyLock<Vec<SchemeAlgorithms>> = LazyLock::new(|| {
    CNSA2_SCHEMES
        .iter()
        .map(|scheme| (scheme.scheme, scheme.algorithms.as_slice()))
        .collect()
});

static CNSA2_SIGNATURES: LazyLock<WebPkiSupportedAlgorithms> =
    LazyLock::new(|| WebPkiSupportedAlgorithms {
        all: CNSA2_ALL.as_slice(),
        mapping: CNSA2_MAPPING.as_slice(),
    });

#[cfg(test)]
mod tests {
    use super::*;

    fn profile_settings(
        profile: CryptoProfile,
        allow_tls12: bool,
        key_share: KeyShare,
    ) -> ProfileSettings {
        ProfileSettings::new(
            profile,
            &TlsProfiles::default().with_tls12(CryptoProfile::Strong, allow_tls12),
            key_share,
        )
    }

    /// Each profile offers its own suites, groups and schemes, in order, and
    /// a classical key share moves the classical groups first.
    #[test]
    fn each_profile_offers_its_own_lists() {
        use rustls::CipherSuite as C;
        use rustls::NamedGroup as G;

        let names = |p: ProfileSettings| {
            let suites: Vec<C> = p.cipher_suites().iter().map(|s| s.suite()).collect();
            let groups: Vec<G> = p.kx_groups().iter().map(|g| g.name()).collect();
            (suites, groups)
        };
        let strong13 = vec![
            C::TLS13_AES_256_GCM_SHA384,
            C::TLS13_CHACHA20_POLY1305_SHA256,
            C::TLS13_AES_128_GCM_SHA256,
        ];
        let strong_groups = vec![
            G::X25519MLKEM768,
            G::secp256r1MLKEM768,
            G::secp384r1,
            G::X25519,
            G::secp256r1,
        ];
        let cases: Vec<(&str, ProfileSettings, Vec<C>, Vec<G>)> = vec![
            (
                "strong",
                profile_settings(CryptoProfile::Strong, false, KeyShare::Profile),
                strong13.clone(),
                strong_groups.clone(),
            ),
            (
                "strong with allow_tls12",
                profile_settings(CryptoProfile::Strong, true, KeyShare::Profile),
                [
                    strong13.as_slice(),
                    &[
                        C::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                        C::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                    ],
                ]
                .concat(),
                strong_groups,
            ),
            (
                "strong, classical share first",
                profile_settings(CryptoProfile::Strong, false, KeyShare::Classical),
                strong13,
                vec![
                    G::secp384r1,
                    G::X25519,
                    G::secp256r1,
                    G::X25519MLKEM768,
                    G::secp256r1MLKEM768,
                ],
            ),
            (
                "commercial",
                profile_settings(CryptoProfile::Commercial, false, KeyShare::Profile),
                vec![
                    C::TLS13_AES_128_GCM_SHA256,
                    C::TLS13_AES_256_GCM_SHA384,
                    C::TLS13_CHACHA20_POLY1305_SHA256,
                    C::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                    C::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
                    C::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
                    C::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                    C::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                    C::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
                ],
                vec![G::X25519MLKEM768, G::X25519, G::secp256r1, G::secp384r1],
            ),
            (
                "cnsa2",
                profile_settings(CryptoProfile::Cnsa2, false, KeyShare::Profile),
                vec![C::TLS13_AES_256_GCM_SHA384],
                vec![G::MLKEM1024, G::secp384r1],
            ),
        ];
        for (name, p, suites, groups) in cases {
            assert_eq!(names(p), (suites, groups), "{name}");
        }

        let cnsa2: Vec<SignatureScheme> =
            profile_settings(CryptoProfile::Cnsa2, false, KeyShare::Profile)
                .signatures()
                .mapping
                .iter()
                .map(|(scheme, _)| *scheme)
                .collect();
        assert_eq!(
            cnsa2,
            [
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ML_DSA_87
            ]
        );
        assert_eq!(
            profile_settings(CryptoProfile::Cnsa2, false, KeyShare::Profile)
                .signatures()
                .all
                .len(),
            2,
            "cnsa2 verifies ECDSA P-384 with SHA-384 and ML-DSA-87 certificates only"
        );
        let strong = profile_settings(CryptoProfile::Strong, false, KeyShare::Profile).signatures();
        assert!(
            strong
                .mapping
                .iter()
                .any(|(s, _)| *s == SignatureScheme::RSA_PKCS1_SHA256),
            "strong verifies RSA chains, such as AD CS RSA-2048"
        );
    }

    /// cnsa2 never offers TLS 1.2, whatever the settings say.
    #[test]
    fn cnsa2_has_no_tls12() {
        let profiles = TlsProfiles::default().with_tls12(CryptoProfile::Cnsa2, true);
        let p = ProfileSettings::new(CryptoProfile::Cnsa2, &profiles, KeyShare::Profile);
        assert!(!p.allow_tls12);
        assert_eq!(p.versions().len(), 1);
    }
}
