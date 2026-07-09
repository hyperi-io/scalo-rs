// Project:   scalo
// File:      src/crypto.rs
// Purpose:   CryptoProfile - the common, opinionated crypto posture the whole
//            suite reads (tls today; secrets/jwt later)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # Crypto posture
//!
//! One opinionated place that defines HyperI's crypto stance, so a consuming
//! crate SELECTS a profile and inherits the algorithm floor, the
//! commercial-grade fallback, and the downgrade warnings - it never
//! hand-assembles a cipher/curve/version policy. [`crate::tls`] is the first
//! consumer; secrets-at-rest and JWT policy read the same profile later.
//!
//! ## Profiles - base is commercial-floor, high-security is opt-in
//!
//! - [`CryptoProfile::Prod`] (**default**): commercial-floor best practice.
//!   TLS 1.2 floor (1.3 preferred), hybrid post-quantum key exchange
//!   **preferred** with classical fallback, AES-256-GCM, verified certs,
//!   warn-on-downgrade. Deployable in ordinary commercial estates (enterprise
//!   middleboxes, legacy clients) without being locked out.
//! - [`CryptoProfile::HighSec`]: national-security opt-in. TLS 1.3 only,
//!   hybrid post-quantum key exchange **required** (peers that cannot do it are
//!   refused).
//! - [`CryptoProfile::DevTest`]: local/dev; same algorithms as prod, warnings
//!   quietened.
//!
//! ## Honest naming
//!
//! The symmetric + hash choices (AES-256-GCM, SHA-384) meet **CNSA 2.0**; the
//! asymmetric choices (ECDH/ECDSA P-384) are the **CNSA 1.0** classical suite.
//! The hybrid ML-KEM key-exchange layer is the feasible-today step toward full
//! CNSA 2.0 (ML-KEM-1024 / ML-DSA-87). We do not label a P-384 baseline
//! "CNSA 2.0".

/// Operating crypto profile. `Prod` is the drop-in default; `HighSec` is opt-in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CryptoProfile {
    /// Commercial-floor best practice (the default). See the module docs.
    #[default]
    Prod,
    /// National-security posture: TLS 1.3 only, post-quantum required.
    HighSec,
    /// Local/dev: prod algorithms, warnings quietened.
    DevTest,
}

/// Post-quantum key-exchange stance for a TLS connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PqcMode {
    /// Offer the hybrid ML-KEM group **first**, fall back to classical if the
    /// peer cannot do it. Real harvest-now-decrypt-later protection where
    /// possible, no interop break where not. The commercial default.
    Prefer,
    /// Offer **only** the hybrid ML-KEM group(s): a peer that cannot do
    /// post-quantum key exchange is refused. National-security opt-in.
    Require,
    /// Classical groups only - for a peer or middlebox that chokes on the
    /// (large) hybrid key share. An explicit, logged downgrade.
    Off,
}

/// TLS protocol-version floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsFloor {
    /// TLS 1.2 minimum, 1.3 preferred - the commercial floor.
    V1_2,
    /// TLS 1.3 only.
    V1_3,
}

impl CryptoProfile {
    /// The TLS version floor this profile mandates.
    #[must_use]
    pub fn tls_floor(self) -> TlsFloor {
        match self {
            // prod + devtest keep the commercial floor so enterprise
            // middleboxes and legacy clients are not locked out.
            CryptoProfile::Prod | CryptoProfile::DevTest => TlsFloor::V1_2,
            CryptoProfile::HighSec => TlsFloor::V1_3,
        }
    }

    /// The post-quantum key-exchange stance this profile mandates.
    #[must_use]
    pub fn pqc(self) -> PqcMode {
        match self {
            CryptoProfile::Prod | CryptoProfile::DevTest => PqcMode::Prefer,
            CryptoProfile::HighSec => PqcMode::Require,
        }
    }

    /// Whether a below-preferred negotiation should emit a warning.
    #[must_use]
    pub fn warn_on_downgrade(self) -> bool {
        !matches!(self, CryptoProfile::DevTest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_prod_commercial_floor() {
        let p = CryptoProfile::default();
        assert_eq!(p, CryptoProfile::Prod);
        assert_eq!(p.tls_floor(), TlsFloor::V1_2);
        assert_eq!(p.pqc(), PqcMode::Prefer);
        assert!(p.warn_on_downgrade());
    }

    #[test]
    fn highsec_is_strict() {
        let p = CryptoProfile::HighSec;
        assert_eq!(p.tls_floor(), TlsFloor::V1_3);
        assert_eq!(p.pqc(), PqcMode::Require);
    }

    #[test]
    fn devtest_quietens_warnings_but_keeps_algorithms() {
        let p = CryptoProfile::DevTest;
        assert_eq!(p.tls_floor(), TlsFloor::V1_2);
        assert_eq!(p.pqc(), PqcMode::Prefer);
        assert!(!p.warn_on_downgrade());
    }
}
