// Project:   scalo
// File:      src/crypto.rs
// Purpose:   TLS profiles: the named crypto postures every TLS client and server selects
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # TLS profiles
//!
//! A connection names a [`CryptoProfile`] and inherits its protocol versions,
//! cipher suites, key-exchange groups and signature schemes. It never
//! assembles a cipher or curve list itself. The `tls` module builds every
//! client and server config from the profile, and Kafka maps it onto
//! librdkafka's OpenSSL lists.
//!
//! | Profile | TLS | Key exchange |
//! | --- | --- | --- |
//! | `strong` (default) | 1.3, and 1.2 only with `allow_tls12` | hybrid post-quantum first |
//! | `commercial` | 1.3 and 1.2 | hybrid post-quantum first |
//! | `cnsa2` | 1.3 only | ML-KEM-1024, then P-384 |
//!
//! TLS 1.2 is a profile setting, set through the cascade for every connection
//! that names the profile, never per connection:
//!
//! ```yaml
//! tls:
//!   profiles:
//!     strong:
//!       allow_tls12: true
//! ```
//!
//! `docs/tls-profiles.md` lists each profile's suites, groups and schemes and
//! what each connects to. FIPS 140-3 is a build mode, not a profile: `fips`
//! fails at config load on a build without it, and no scalo build has it yet.

use serde::{Deserialize, Serialize};

/// A named TLS posture. `strong` is the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum CryptoProfile {
    /// TLS 1.3 only unless the profile sets `allow_tls12`, AES-256-GCM,
    /// ChaCha20 and AES-128-GCM, hybrid post-quantum key exchange first.
    #[default]
    Strong,
    /// TLS 1.3 and 1.2, every ECDHE AEAD suite, hybrid post-quantum key
    /// exchange first: the cheapest posture that is still current.
    Commercial,
    /// TLS 1.3 only, AES-256-GCM, ML-KEM-1024 then P-384, ECDSA P-384 and
    /// ML-DSA-87 signatures. The CNSA 2.0 shape.
    Cnsa2,
}

impl CryptoProfile {
    /// Every profile, in the order the docs list them.
    pub const ALL: [Self; 3] = [Self::Strong, Self::Commercial, Self::Cnsa2];

    /// The name config uses.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Commercial => "commercial",
            Self::Cnsa2 => "cnsa2",
        }
    }

    /// The profile to try when a peer refuses every suite and group this one
    /// offers, or `None` when no wider profile exists.
    #[must_use]
    pub fn wider(self) -> Option<Self> {
        match self {
            Self::Cnsa2 => Some(Self::Strong),
            Self::Strong => Some(Self::Commercial),
            Self::Commercial => None,
        }
    }

    /// Whether the profile can turn TLS 1.2 on at all.
    #[must_use]
    pub fn has_tls12(self) -> bool {
        !matches!(self, Self::Cnsa2)
    }
}

impl std::fmt::Display for CryptoProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A profile name that is not one this build has.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProfileNameError {
    /// `fips` names a build mode this build lacks.
    #[error(
        "TLS profile `fips` needs a FIPS 140-3 build of scalo, and this build is not one: \
         choose strong, commercial or cnsa2"
    )]
    NoFipsBuild,
    /// No profile has this name.
    #[error("unknown TLS profile `{0}`: choose strong, commercial or cnsa2")]
    Unknown(String),
}

impl std::str::FromStr for CryptoProfile {
    type Err = ProfileNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "strong" => Ok(Self::Strong),
            "commercial" => Ok(Self::Commercial),
            "cnsa2" => Ok(Self::Cnsa2),
            "fips" => Err(ProfileNameError::NoFipsBuild),
            other => Err(ProfileNameError::Unknown(other.to_string())),
        }
    }
}

impl<'de> Deserialize<'de> for CryptoProfile {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        name.parse().map_err(serde::de::Error::custom)
    }
}

/// Which key share a client sends first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum KeyShare {
    /// The profile's order: a post-quantum group first where it has one.
    #[default]
    Profile,
    /// A classical group first, with the post-quantum groups listed but not
    /// shared, for a receiver behind a middlebox that drops the larger
    /// ClientHello. A server that prefers a post-quantum group asks for it in
    /// a second round trip.
    Classical,
}

/// Settings one profile takes from the `tls.profiles` cascade section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ProfileOptions {
    /// Negotiate TLS 1.2 with a peer that has no TLS 1.3, using only the
    /// profile's own TLS 1.2 suites and groups. Each TLS 1.2 handshake logs a
    /// warning and counts `tls_tls12_handshakes_total`.
    pub allow_tls12: bool,
}

/// The `tls.profiles` cascade section: per-profile settings for every
/// connection that names the profile.
///
/// `strong` and `cnsa2` default to TLS 1.3 only, `commercial` to TLS 1.3 and
/// 1.2. `cnsa2` refuses `allow_tls12`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default, deny_unknown_fields)]
pub struct TlsProfiles {
    /// The `strong` profile.
    pub strong: ProfileOptions,
    /// The `commercial` profile.
    pub commercial: ProfileOptions,
    /// The `cnsa2` profile.
    pub cnsa2: ProfileOptions,
}

impl Default for TlsProfiles {
    fn default() -> Self {
        Self {
            strong: ProfileOptions { allow_tls12: false },
            commercial: ProfileOptions { allow_tls12: true },
            cnsa2: ProfileOptions { allow_tls12: false },
        }
    }
}

/// The `tls` cascade section, of which `profiles` is the only key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct TlsSection {
    profiles: TlsProfiles,
}

impl TlsProfiles {
    /// The settings under the `tls` cascade key, or the defaults when the
    /// cascade is not set up or the key is absent.
    ///
    /// # Errors
    ///
    /// The section does not parse, or sets `allow_tls12` on `cnsa2`.
    pub fn from_cascade() -> Result<Self, String> {
        let section: TlsSection =
            crate::cascade::section("tls").map_err(|e| format!("TLS profiles: {e}"))?;
        section.profiles.validate()?;
        Ok(section.profiles)
    }

    /// Refuse settings no profile can honour.
    ///
    /// # Errors
    ///
    /// `allow_tls12` on `cnsa2`, which has no TLS 1.2 suite.
    pub fn validate(&self) -> Result<(), String> {
        if self.cnsa2.allow_tls12 {
            return Err(
                "TLS profiles: cnsa2 is TLS 1.3 only, so it takes no allow_tls12".to_string(),
            );
        }
        Ok(())
    }

    /// The settings of `profile`.
    #[must_use]
    pub fn options(&self, profile: CryptoProfile) -> ProfileOptions {
        match profile {
            CryptoProfile::Strong => self.strong,
            CryptoProfile::Commercial => self.commercial,
            CryptoProfile::Cnsa2 => self.cnsa2,
        }
    }

    /// These settings with `allow_tls12` set on `profile`.
    #[must_use]
    pub fn with_tls12(mut self, profile: CryptoProfile, allow: bool) -> Self {
        let options = ProfileOptions { allow_tls12: allow };
        match profile {
            CryptoProfile::Strong => self.strong = options,
            CryptoProfile::Commercial => self.commercial = options,
            CryptoProfile::Cnsa2 => self.cnsa2 = options,
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each name reads as its profile, `fips` and any other name fail with
    /// the reason, and the default is strong with TLS 1.3 only.
    #[test]
    fn profile_names_read_and_fips_fails_at_load() {
        use serde::de::value::{Error, StrDeserializer};

        let read = |name: &str| CryptoProfile::deserialize(StrDeserializer::<Error>::new(name));
        for profile in CryptoProfile::ALL {
            assert_eq!(read(profile.as_str()).unwrap(), profile);
        }
        for (name, reason) in [
            ("fips", "needs a FIPS 140-3 build"),
            ("highsec", "unknown TLS profile `highsec`"),
            ("Strong", "unknown TLS profile `Strong`"),
        ] {
            let error = read(name).unwrap_err().to_string();
            assert!(error.contains(reason), "{name}: {error}");
        }
        assert_eq!(CryptoProfile::default(), CryptoProfile::Strong);
        let defaults = TlsProfiles::default();
        assert!(!defaults.options(CryptoProfile::Strong).allow_tls12);
        assert!(defaults.options(CryptoProfile::Commercial).allow_tls12);
        assert!(!defaults.options(CryptoProfile::Cnsa2).allow_tls12);
    }

    /// `allow_tls12` reaches a profile through the cascade, the others keep
    /// their defaults, and cnsa2 refuses it.
    #[cfg(feature = "config")]
    #[test]
    fn allow_tls12_reaches_a_profile_through_the_cascade() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "tls:\n  profiles:\n    strong:\n      allow_tls12: true\n",
        )
        .unwrap();
        crate::config::setup(crate::config::ConfigOptions {
            env_prefix: "TLSPROFILECFG".to_string(),
            config_paths: vec![dir.path().to_path_buf()],
            search_standard_paths: false,
            load_dotenv: false,
            ..crate::config::ConfigOptions::default()
        })
        .unwrap();
        let profiles = TlsProfiles::from_cascade().unwrap();
        assert_eq!(
            profiles,
            TlsProfiles::default().with_tls12(CryptoProfile::Strong, true)
        );

        let refused = TlsProfiles::default().with_tls12(CryptoProfile::Cnsa2, true);
        assert!(
            refused
                .validate()
                .unwrap_err()
                .contains("cnsa2 is TLS 1.3 only")
        );
    }
}
