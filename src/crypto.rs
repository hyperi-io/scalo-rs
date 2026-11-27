// Project:   scalo
// File:      src/crypto.rs
// Purpose:   TLS profiles, which every TLS client and server selects by name
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

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// A named TLS profile. `strong` is the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum CryptoProfile {
    /// TLS 1.3 only unless the profile sets `allow_tls12`, AES-256-GCM,
    /// ChaCha20 and AES-128-GCM, hybrid post-quantum key exchange first.
    #[default]
    Strong,
    /// TLS 1.3 and 1.2, every ECDHE AEAD suite, hybrid post-quantum key
    /// exchange first: the cheapest profile that is still current.
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

/// The app default, set once by [`TlsProfiles::set_app_default`].
static APP_DEFAULT: OnceLock<TlsProfiles> = OnceLock::new();

/// Why [`TlsProfiles::set_app_default`] refused its settings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AppDefaultError {
    /// The app default was already set, and the first call stands.
    #[error("TLS profiles app default is already set")]
    AlreadySet,
    /// The settings fail [`TlsProfiles::validate`].
    #[error("{0}")]
    Invalid(String),
}

/// The `tls` cascade section, of which `profiles` is the only key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct TlsSection {
    profiles: SiteProfiles,
}

/// The profiles a `tls.profiles` section sets. A profile it leaves out keeps the app default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SiteProfiles {
    strong: Option<ProfileOptions>,
    commercial: Option<ProfileOptions>,
    cnsa2: Option<ProfileOptions>,
}

impl SiteProfiles {
    /// `base` with each profile this section sets replaced.
    fn over(self, base: TlsProfiles) -> TlsProfiles {
        TlsProfiles {
            strong: self.strong.unwrap_or(base.strong),
            commercial: self.commercial.unwrap_or(base.commercial),
            cnsa2: self.cnsa2.unwrap_or(base.cnsa2),
        }
    }
}

impl TlsProfiles {
    /// The settings under the `tls` cascade key, over the app default. The
    /// app default applies when the cascade is not set up or the key is absent.
    ///
    /// # Errors
    ///
    /// The section does not parse, or sets `allow_tls12` on `cnsa2`.
    pub fn from_cascade() -> Result<Self, String> {
        let section: TlsSection =
            crate::cascade::section("tls").map_err(|e| format!("TLS profiles: {e}"))?;
        let profiles = section.profiles.over(Self::app_default());
        profiles.validate()?;
        Ok(profiles)
    }

    /// Set the settings the cascade falls back to for every profile the
    /// `tls.profiles` section leaves out. The built-in defaults apply where
    /// this is never called.
    ///
    /// Call it once, before any TLS config or transport is built: a config
    /// built earlier keeps the settings it read. A site's `tls.profiles`
    /// still wins over these.
    ///
    /// # Errors
    ///
    /// [`AppDefaultError::Invalid`] when [`validate`](Self::validate) refuses
    /// the settings, and [`AppDefaultError::AlreadySet`] on a second call.
    /// Neither leaves the app default changed.
    pub fn set_app_default(profiles: Self) -> Result<(), AppDefaultError> {
        profiles.validate().map_err(AppDefaultError::Invalid)?;
        APP_DEFAULT
            .set(profiles)
            .map_err(|_| AppDefaultError::AlreadySet)
    }

    /// The app default, or the built-in defaults when none was set.
    pub(crate) fn app_default() -> Self {
        APP_DEFAULT.get().copied().unwrap_or_default()
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

    /// Load a `settings.yaml` holding `yaml` as the cascade. The config is
    /// read here, so the temporary directory can go. A test that sets a
    /// process global runs in its own process under nextest.
    #[cfg(feature = "config")]
    fn setup_cascade(yaml: &str) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("settings.yaml"), yaml).unwrap();
        crate::config::setup(crate::config::ConfigOptions {
            env_prefix: "TLSPROFILECFG".to_string(),
            config_paths: vec![dir.path().to_path_buf()],
            search_standard_paths: false,
            load_dotenv: false,
            ..crate::config::ConfigOptions::default()
        })
        .unwrap();
    }

    /// `allow_tls12` reaches a profile through the cascade, the others keep
    /// their defaults, and cnsa2 refuses it.
    #[cfg(feature = "config")]
    #[test]
    fn allow_tls12_reaches_a_profile_through_the_cascade() {
        setup_cascade("tls:\n  profiles:\n    strong:\n      allow_tls12: true\n");
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

    /// The app default is set once, settings no profile can honour are
    /// refused without taking the slot, and a second call names the error.
    #[test]
    fn app_default_is_set_once_and_refuses_invalid_settings() {
        let refused = TlsProfiles::default().with_tls12(CryptoProfile::Cnsa2, true);
        assert!(matches!(
            TlsProfiles::set_app_default(refused),
            Err(AppDefaultError::Invalid(_))
        ));

        let allow = TlsProfiles::default().with_tls12(CryptoProfile::Strong, true);
        assert_eq!(TlsProfiles::set_app_default(allow), Ok(()));
        assert_eq!(
            TlsProfiles::set_app_default(TlsProfiles::default()),
            Err(AppDefaultError::AlreadySet)
        );
        assert_eq!(TlsProfiles::app_default(), allow);
    }

    /// The app default fills each profile a `tls.profiles` section leaves
    /// out, and the section's own setting for a profile it sets.
    #[cfg(feature = "config")]
    #[test]
    fn the_cascade_section_wins_over_the_app_default_per_profile() {
        TlsProfiles::set_app_default(
            TlsProfiles::default()
                .with_tls12(CryptoProfile::Strong, true)
                .with_tls12(CryptoProfile::Commercial, false),
        )
        .unwrap();
        setup_cascade("tls:\n  profiles:\n    strong:\n      allow_tls12: false\n");

        // strong from the section, commercial from the app default (the built-in is true).
        let profiles = TlsProfiles::from_cascade().unwrap();
        assert_eq!(
            profiles,
            TlsProfiles::default()
                .with_tls12(CryptoProfile::Strong, false)
                .with_tls12(CryptoProfile::Commercial, false)
        );
    }
}
