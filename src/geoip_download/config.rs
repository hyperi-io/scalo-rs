// Project:   scalo
// File:      src/geoip_download/config.rs
// Purpose:   GeoIP database provisioning configuration types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration for GeoIP database provisioning.
//!
//! Describes WHICH MMDB databases a service wants and HOW to obtain them.
//! It carries nothing about what the databases are used for -- lookup caches,
//! field mappings and enrichment toggles stay in the consuming application.
//!
//! ## Config cascade example
//!
//! ```yaml
//! geoip:
//!   enabled: true
//!   provider: db_ip_lite
//!   # Explicit paths override the provider entirely (no download attempted).
//!   city_db_path: null
//!   asn_db_path: null
//!   auto_download:
//!     enabled: true
//!     data_dir: /var/lib/geoip
//!     max_age_days: 30
//!     # Credentials are SensitiveString: redacted in Debug and in any
//!     # serialised form. Supply them through the secrets layer, not literals.
//!     maxmind_account_id: null
//!     maxmind_license_key: null
//!     ipinfo_token: null
//! ```

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::sensitive::SensitiveString;

/// Default directory for downloaded MMDB files.
const DEFAULT_DATA_DIR: &str = "/var/lib/geoip";

/// Default staleness threshold, in days, before a re-download is attempted.
const DEFAULT_MAX_AGE_DAYS: u32 = 30;

/// Source of the MMDB databases.
///
/// | Variant | Auth | City | ASN |
/// |---|---|---|---|
/// | [`DbIpLite`](Self::DbIpLite) | none | yes | yes |
/// | [`MaxMindGeoLite2`](Self::MaxMindGeoLite2) | HTTP basic | yes | yes |
/// | [`IpLocate`](Self::IpLocate) | none | no | yes |
/// | [`IpInfoLite`](Self::IpInfoLite) | token | yes | no |
/// | [`Sapics`](Self::Sapics) | none | no | yes |
/// | [`Custom`](Self::Custom) | n/a | explicit path | explicit path |
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GeoIpProvider {
    /// DB-IP Lite -- free, anonymous download, city-level, CC BY 4.0.
    #[default]
    DbIpLite,

    /// MaxMind GeoLite2 -- free account required (account id + licence key).
    MaxMindGeoLite2,

    /// IPLocate.io -- free, anonymous, country + ASN only.
    IpLocate,

    /// IPinfo Lite -- free token required, country + ASN only.
    IpInfoLite,

    /// sapics/ip-location-db -- free CC0, country + ASN only.
    Sapics,

    /// Caller supplies the MMDB paths directly; nothing is downloaded.
    Custom,
}

/// Auto-download settings.
///
/// The three credential fields are [`SensitiveString`], so they are redacted
/// in `Debug` output and in any serialised form (config dumps, JSON schema
/// examples, error reports). Only the download call itself exposes them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct AutoDownloadConfig {
    /// Download a database when the local copy is missing or stale.
    ///
    /// When false, [`ensure_databases`](super::ensure_databases) returns only
    /// the files that already exist on disk.
    pub enabled: bool,

    /// Directory the downloaded MMDB files are written to.
    pub data_dir: PathBuf,

    /// MaxMind account id, required by
    /// [`GeoIpProvider::MaxMindGeoLite2`].
    pub maxmind_account_id: Option<SensitiveString>,

    /// MaxMind licence key, required by
    /// [`GeoIpProvider::MaxMindGeoLite2`].
    pub maxmind_license_key: Option<SensitiveString>,

    /// IPinfo API token, required by [`GeoIpProvider::IpInfoLite`].
    pub ipinfo_token: Option<SensitiveString>,

    /// Age, in days, past which a local database is treated as stale.
    pub max_age_days: u32,
}

impl Default for AutoDownloadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            maxmind_account_id: None,
            maxmind_license_key: None,
            ipinfo_token: None,
            max_age_days: DEFAULT_MAX_AGE_DAYS,
        }
    }
}

/// GeoIP database provisioning configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct GeoIpConfig {
    /// Provision databases at all. Defaults to true: calling
    /// [`ensure_databases`](super::ensure_databases) is the opt-in, so this is
    /// the config-side opt-out for an app that wires the call unconditionally.
    pub enabled: bool,

    /// Where the databases come from.
    pub provider: GeoIpProvider,

    /// Explicit city MMDB path. Set on either path field and the provider is
    /// bypassed entirely -- nothing is downloaded and nothing is checked.
    pub city_db_path: Option<PathBuf>,

    /// Explicit ASN MMDB path. See [`city_db_path`](Self::city_db_path).
    pub asn_db_path: Option<PathBuf>,

    /// Auto-download settings.
    pub auto_download: AutoDownloadConfig,
}

impl Default for GeoIpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            provider: GeoIpProvider::default(),
            city_db_path: None,
            asn_db_path: None,
            auto_download: AutoDownloadConfig::default(),
        }
    }
}

impl GeoIpConfig {
    /// Load from the config cascade under the `geoip` key, else defaults.
    #[must_use]
    pub fn from_cascade() -> Self {
        #[cfg(feature = "config")]
        {
            if let Some(cfg) = crate::config::try_get()
                && let Ok(geoip) = cfg.unmarshal_key_registered::<Self>("geoip")
            {
                return geoip;
            }
        }
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_default_is_dbip_lite() {
        assert_eq!(GeoIpProvider::default(), GeoIpProvider::DbIpLite);
    }

    #[test]
    fn provider_variants_compare_distinctly() {
        assert_eq!(GeoIpProvider::DbIpLite, GeoIpProvider::DbIpLite);
        assert_ne!(GeoIpProvider::DbIpLite, GeoIpProvider::MaxMindGeoLite2);
        assert_ne!(GeoIpProvider::Custom, GeoIpProvider::Sapics);
        assert_ne!(GeoIpProvider::IpLocate, GeoIpProvider::IpInfoLite);
    }

    #[test]
    fn provider_round_trips_through_snake_case() {
        for (provider, wire) in [
            (GeoIpProvider::DbIpLite, "db_ip_lite"),
            (GeoIpProvider::MaxMindGeoLite2, "max_mind_geo_lite2"),
            (GeoIpProvider::IpLocate, "ip_locate"),
            (GeoIpProvider::IpInfoLite, "ip_info_lite"),
            (GeoIpProvider::Sapics, "sapics"),
            (GeoIpProvider::Custom, "custom"),
        ] {
            let quoted = format!("\"{wire}\"");
            assert_eq!(serde_json::to_string(&provider).unwrap(), quoted);
            let decoded: GeoIpProvider = serde_json::from_str(&quoted).unwrap();
            assert_eq!(decoded, provider);
        }
    }

    #[test]
    fn auto_download_defaults() {
        let auto = AutoDownloadConfig::default();
        assert!(auto.enabled);
        assert_eq!(auto.data_dir, PathBuf::from("/var/lib/geoip"));
        assert_eq!(auto.max_age_days, 30);
        assert!(auto.maxmind_account_id.is_none());
        assert!(auto.maxmind_license_key.is_none());
        assert!(auto.ipinfo_token.is_none());
    }

    #[test]
    fn geoip_defaults() {
        let config = GeoIpConfig::default();
        assert!(config.enabled);
        assert_eq!(config.provider, GeoIpProvider::DbIpLite);
        assert!(config.city_db_path.is_none());
        assert!(config.asn_db_path.is_none());
        assert!(config.auto_download.enabled);
    }

    #[test]
    fn deserialises_with_partial_keys() {
        let json = r#"{
            "provider": "max_mind_geo_lite2",
            "auto_download": {
                "data_dir": "/srv/geoip",
                "max_age_days": 7,
                "maxmind_account_id": "123456",
                "maxmind_license_key": "secret-key"
            }
        }"#;
        let config: GeoIpConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.provider, GeoIpProvider::MaxMindGeoLite2);
        assert_eq!(config.auto_download.data_dir, PathBuf::from("/srv/geoip"));
        assert_eq!(config.auto_download.max_age_days, 7);
        assert_eq!(
            config
                .auto_download
                .maxmind_license_key
                .as_ref()
                .map(SensitiveString::expose),
            Some("secret-key")
        );
        // Unset keys fall back to the struct defaults, not to zero values.
        assert!(config.enabled);
        assert!(config.auto_download.enabled);
    }

    #[test]
    fn credentials_are_redacted_in_debug() {
        let config = GeoIpConfig {
            auto_download: AutoDownloadConfig {
                maxmind_account_id: Some("account-1234".into()),
                maxmind_license_key: Some("licence-abcd".into()),
                ipinfo_token: Some("token-wxyz".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("account-1234"), "{rendered}");
        assert!(!rendered.contains("licence-abcd"), "{rendered}");
        assert!(!rendered.contains("token-wxyz"), "{rendered}");
        assert!(rendered.contains("REDACTED"), "{rendered}");
    }

    #[test]
    fn credentials_are_redacted_when_serialised() {
        let config = GeoIpConfig {
            auto_download: AutoDownloadConfig {
                maxmind_license_key: Some("licence-abcd".into()),
                ipinfo_token: Some("token-wxyz".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let dumped = serde_json::to_string(&config).unwrap();
        assert!(!dumped.contains("licence-abcd"), "{dumped}");
        assert!(!dumped.contains("token-wxyz"), "{dumped}");
    }

    #[test]
    fn from_cascade_falls_back_to_defaults() {
        // No cascade is initialised in the unit-test process, so this exercises
        // the fallback arm rather than the config lookup.
        let config = GeoIpConfig::from_cascade();
        assert_eq!(config.provider, GeoIpProvider::DbIpLite);
        assert!(config.enabled);
    }
}
