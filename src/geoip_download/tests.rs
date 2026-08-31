// Project:   scalo
// File:      src/geoip_download/tests.rs
// Purpose:   Offline coverage for GeoIP database provisioning
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Every test here stays offline.
//!
//! The provider endpoints are real third-party services; hitting them from a
//! test suite would make it slow, flaky and rude. The paths exercised are the
//! ones that decide an outcome before any socket is opened: explicit-path
//! override, existing-file reuse, auto-download disabled, freshness, missing
//! credentials, and providers that do not publish a given database kind.

use std::fs;
use std::path::{Path, PathBuf};

use super::*;

/// Config pointed at a scratch directory, with auto-download on.
fn config_in(dir: &Path, provider: GeoIpProvider) -> GeoIpConfig {
    GeoIpConfig {
        provider,
        auto_download: AutoDownloadConfig {
            enabled: true,
            data_dir: dir.to_path_buf(),
            ..Default::default()
        },
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// provider_filenames
// ---------------------------------------------------------------------------

#[test]
fn provider_filenames_cover_every_variant() {
    let cases = [
        (
            GeoIpProvider::DbIpLite,
            Some("dbip-city-lite.mmdb"),
            Some("dbip-asn-lite.mmdb"),
        ),
        (
            GeoIpProvider::MaxMindGeoLite2,
            Some("GeoLite2-City.mmdb"),
            Some("GeoLite2-ASN.mmdb"),
        ),
        (GeoIpProvider::IpLocate, None, Some("iplocate-asn.mmdb")),
        (GeoIpProvider::IpInfoLite, Some("ipinfo-lite.mmdb"), None),
        (GeoIpProvider::Sapics, None, Some("sapics-asn-country.mmdb")),
        (GeoIpProvider::Custom, None, None),
    ];

    for (provider, city, asn) in cases {
        assert_eq!(provider_filenames(provider), (city, asn), "{provider:?}");
    }
}

#[test]
fn plan_destinations_match_the_expected_filenames() {
    // The freshness check reads provider_filenames while the download writes
    // whatever plan() chose. A mismatch would re-download every startup.
    let dir = PathBuf::from("/var/lib/geoip");
    for provider in [
        GeoIpProvider::DbIpLite,
        GeoIpProvider::MaxMindGeoLite2,
        GeoIpProvider::IpLocate,
        GeoIpProvider::IpInfoLite,
        GeoIpProvider::Sapics,
    ] {
        let mut config = config_in(&dir, provider);
        config.auto_download.maxmind_account_id = Some("account".into());
        config.auto_download.maxmind_license_key = Some("licence".into());
        config.auto_download.ipinfo_token = Some("token".into());

        let (city, asn) = provider_filenames(provider);
        for (kind, expected) in [(Kind::City, city), (Kind::Asn, asn)] {
            match (plan(kind, &config), expected) {
                (Ok(transfer), Some(name)) => {
                    assert_eq!(transfer.dest, dir.join(name), "{provider:?} {kind:?}");
                }
                (Err(GeoIpDownloadError::NoDatabases { .. }), None) => {}
                (result, expected) => {
                    panic!("{provider:?} {kind:?}: {result:?} does not match {expected:?}")
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// is_fresh
// ---------------------------------------------------------------------------

#[test]
fn is_fresh_rejects_a_missing_file() {
    let missing = Path::new("/nonexistent/path/file.mmdb");
    assert!(!is_fresh(missing, 0));
    assert!(!is_fresh(missing, 86_400));
    assert!(!is_fresh(missing, u64::MAX));
}

#[test]
fn is_fresh_accepts_a_new_file() {
    let file = tempfile::NamedTempFile::new().unwrap();
    assert!(is_fresh(file.path(), 86_400));
    assert!(is_fresh(file.path(), u64::MAX / 2));
}

#[test]
fn is_fresh_rejects_everything_at_zero_max_age() {
    let file = tempfile::NamedTempFile::new().unwrap();
    assert!(!is_fresh(file.path(), 0));
}

#[test]
fn is_fresh_handles_a_directory_without_panicking() {
    let dir = tempfile::tempdir().unwrap();
    assert!(is_fresh(dir.path(), 86_400));
}

// ---------------------------------------------------------------------------
// Explicit paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn custom_provider_returns_the_explicit_paths() {
    let config = GeoIpConfig {
        provider: GeoIpProvider::Custom,
        city_db_path: Some("/data/city.mmdb".into()),
        asn_db_path: Some("/data/asn.mmdb".into()),
        ..Default::default()
    };

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths.city, Some(PathBuf::from("/data/city.mmdb")));
    assert_eq!(paths.asn, Some(PathBuf::from("/data/asn.mmdb")));
}

#[tokio::test]
async fn explicit_paths_override_the_provider() {
    let config = GeoIpConfig {
        provider: GeoIpProvider::DbIpLite,
        city_db_path: Some("/custom/city.mmdb".into()),
        asn_db_path: Some("/custom/asn.mmdb".into()),
        ..Default::default()
    };

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths.city, Some(PathBuf::from("/custom/city.mmdb")));
    assert_eq!(paths.asn, Some(PathBuf::from("/custom/asn.mmdb")));
}

#[tokio::test]
async fn custom_provider_without_paths_returns_nothing() {
    let config = GeoIpConfig {
        provider: GeoIpProvider::Custom,
        ..Default::default()
    };

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths, DatabasePaths::default());
}

#[tokio::test]
async fn a_single_explicit_path_still_bypasses_the_provider() {
    let config = GeoIpConfig {
        provider: GeoIpProvider::Custom,
        asn_db_path: Some("/opt/asn.mmdb".into()),
        ..Default::default()
    };

    let paths = ensure_databases(&config).await.unwrap();
    assert!(paths.city.is_none());
    assert_eq!(paths.asn, Some(PathBuf::from("/opt/asn.mmdb")));
}

// ---------------------------------------------------------------------------
// enabled / auto_download gates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disabled_config_resolves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("dbip-city-lite.mmdb"), b"fake mmdb").unwrap();

    let mut config = config_in(dir.path(), GeoIpProvider::DbIpLite);
    config.enabled = false;

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths, DatabasePaths::default());
}

#[tokio::test]
async fn auto_download_off_returns_only_existing_files() {
    let dir = tempfile::tempdir().unwrap();
    let city = dir.path().join("dbip-city-lite.mmdb");
    fs::write(&city, b"fake mmdb").unwrap();
    // The ASN file is deliberately absent.

    let mut config = config_in(dir.path(), GeoIpProvider::DbIpLite);
    config.auto_download.enabled = false;

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths.city, Some(city));
    assert!(paths.asn.is_none());
}

#[tokio::test]
async fn auto_download_off_over_an_empty_directory_returns_nothing() {
    let mut config = config_in(Path::new("/nonexistent"), GeoIpProvider::DbIpLite);
    config.auto_download.enabled = false;

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths, DatabasePaths::default());
}

#[tokio::test]
async fn fresh_files_are_reused_without_a_download() {
    // auto_download is ON here. Both files are new, so freshness short-circuits
    // before any network call -- which is what keeps this test offline.
    let dir = tempfile::tempdir().unwrap();
    let city = dir.path().join("dbip-city-lite.mmdb");
    let asn = dir.path().join("dbip-asn-lite.mmdb");
    fs::write(&city, b"fake city").unwrap();
    fs::write(&asn, b"fake asn").unwrap();

    let config = config_in(dir.path(), GeoIpProvider::DbIpLite);

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths.city, Some(city));
    assert_eq!(paths.asn, Some(asn));
}

// ---------------------------------------------------------------------------
// Missing credentials
// ---------------------------------------------------------------------------

#[tokio::test]
async fn maxmind_without_credentials_degrades_to_no_paths() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path(), GeoIpProvider::MaxMindGeoLite2);

    // Both downloads fail at the credential check, before any socket is opened.
    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths, DatabasePaths::default());
}

#[tokio::test]
async fn ipinfo_without_a_token_degrades_to_no_paths() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path(), GeoIpProvider::IpInfoLite);

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths, DatabasePaths::default());
}

#[tokio::test]
async fn a_stale_file_survives_a_failed_download() {
    // max_age_days = 0 makes every file stale, and the missing MaxMind
    // credentials make the download fail without a network call. The stale
    // file must still come back: degraded enrichment beats none.
    let dir = tempfile::tempdir().unwrap();
    let city = dir.path().join("GeoLite2-City.mmdb");
    fs::write(&city, b"stale city").unwrap();

    let mut config = config_in(dir.path(), GeoIpProvider::MaxMindGeoLite2);
    config.auto_download.max_age_days = 0;

    let paths = ensure_databases(&config).await.unwrap();
    assert_eq!(paths.city, Some(city));
    assert!(paths.asn.is_none(), "no stale ASN file exists");
}

#[test]
fn missing_credentials_name_the_config_field() {
    let dir = PathBuf::from("/var/lib/geoip");

    let err = plan(Kind::City, &config_in(&dir, GeoIpProvider::MaxMindGeoLite2)).unwrap_err();
    assert!(matches!(
        err,
        GeoIpDownloadError::MissingCredential {
            provider: "MaxMindGeoLite2",
            field: "auto_download.maxmind_account_id",
        }
    ));

    let mut config = config_in(&dir, GeoIpProvider::MaxMindGeoLite2);
    config.auto_download.maxmind_account_id = Some("account".into());
    let err = plan(Kind::Asn, &config).unwrap_err();
    assert!(matches!(
        err,
        GeoIpDownloadError::MissingCredential {
            field: "auto_download.maxmind_license_key",
            ..
        }
    ));

    let err = plan(Kind::City, &config_in(&dir, GeoIpProvider::IpInfoLite)).unwrap_err();
    assert!(matches!(
        err,
        GeoIpDownloadError::MissingCredential {
            provider: "IpInfoLite",
            field: "auto_download.ipinfo_token",
        }
    ));
}

// ---------------------------------------------------------------------------
// Providers that publish only one kind
// ---------------------------------------------------------------------------

#[tokio::test]
async fn iplocate_and_sapics_publish_no_city_database() {
    for provider in [GeoIpProvider::IpLocate, GeoIpProvider::Sapics] {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config_in(dir.path(), provider);
        // Off, so the ASN half does not reach for the network.
        config.auto_download.enabled = false;

        let paths = ensure_databases(&config).await.unwrap();
        assert!(paths.city.is_none(), "{provider:?}");
    }
}

#[tokio::test]
async fn ipinfo_lite_publishes_no_asn_database() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(dir.path(), GeoIpProvider::IpInfoLite);
    config.auto_download.enabled = false;

    let paths = ensure_databases(&config).await.unwrap();
    assert!(paths.asn.is_none());
}

// ---------------------------------------------------------------------------
// DatabasePaths + errors
// ---------------------------------------------------------------------------

#[test]
fn database_paths_default_is_empty() {
    let paths = DatabasePaths::default();
    assert!(paths.city.is_none());
    assert!(paths.asn.is_none());
}

#[test]
fn database_paths_hold_either_half() {
    let paths = DatabasePaths {
        city: Some(PathBuf::from("/tmp/city.mmdb")),
        asn: None,
    };
    assert_eq!(paths.city, Some(PathBuf::from("/tmp/city.mmdb")));
    assert!(paths.asn.is_none());
}

#[test]
fn error_messages_name_the_problem() {
    let err = GeoIpDownloadError::MissingCredential {
        provider: "MaxMindGeoLite2",
        field: "auto_download.maxmind_account_id",
    };
    let msg = err.to_string();
    assert!(msg.contains("MaxMindGeoLite2"), "{msg}");
    assert!(msg.contains("auto_download.maxmind_account_id"), "{msg}");

    let err = GeoIpDownloadError::NoDatabases {
        provider: "IpInfoLite".to_string(),
        kind: "asn",
    };
    assert!(err.to_string().contains("IpInfoLite"));

    let err = GeoIpDownloadError::ArchiveMemberMissing {
        member: "GeoLite2-City.mmdb",
    };
    assert!(err.to_string().contains("GeoLite2-City.mmdb"));

    let err = GeoIpDownloadError::UnexpectedStatus {
        url: "https://example.invalid/db".to_string(),
        status: 403,
    };
    assert!(err.to_string().contains("403"));
}

#[test]
fn a_plan_error_never_carries_a_credential() {
    // Error values are logged verbatim by the non-fatal path, so they must not
    // become the leak the SensitiveString wrapper is there to prevent.
    let mut config = config_in(Path::new("/var/lib/geoip"), GeoIpProvider::MaxMindGeoLite2);
    config.auto_download.maxmind_account_id = Some("account-1234".into());

    let err = plan(Kind::City, &config).unwrap_err();
    let rendered = format!("{err} {err:?}");
    assert!(!rendered.contains("account-1234"), "{rendered}");
}
