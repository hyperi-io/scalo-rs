// Project:   scalo
// File:      src/geoip_download/mod.rs
// Purpose:   GeoIP MMDB database provisioning (resolve, freshness, download)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! GeoIP database provisioning.
//!
//! Resolves the MMDB files a service needs and downloads them when the local
//! copy is missing or stale. This module provisions FILES -- it contains no
//! lookup engine. The returned paths are handed to whatever reader the caller
//! uses (an MMDB reader, a VRL enrichment table, a sidecar).
//!
//! # Non-fatal by contract
//!
//! [`ensure_databases`] returns `Ok` with `None` paths when a download fails.
//! A GeoIP database going missing degrades enrichment; it must not stop a
//! service from starting. Failures are logged at `warn` and a stale local file
//! is preferred over no file at all.
//!
//! # Credentials
//!
//! MaxMind and IPinfo credentials are [`crate::SensitiveString`]
//! and never reach a log line. The IPinfo token is attached as a query
//! parameter by the request builder rather than being formatted into the URL
//! string, so the URL this module logs cannot carry it.
//!
//! # Example
//!
//! ```rust,no_run
//! use scalo::geoip_download::{GeoIpConfig, ensure_databases};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let paths = ensure_databases(&GeoIpConfig::from_cascade()).await?;
//! if let Some(city) = paths.city {
//!     println!("city database at {}", city.display());
//! }
//! # Ok(())
//! # }
//! ```

pub mod config;
mod fetch;

pub use config::{AutoDownloadConfig, GeoIpConfig, GeoIpProvider};

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tracing::{debug, warn};

use crate::http_client::{HttpClientError, HttpError};
use crate::sensitive::SensitiveString;
use fetch::{Archive, Credential, Transfer};

/// Seconds in a day, for the staleness comparison.
const SECS_PER_DAY: u64 = 86_400;

/// Timeout for a single database download. MMDB files run to hundreds of MB
/// on a slow link, so the shared 30s default is far too tight.
const DOWNLOAD_TIMEOUT_SECS: u64 = 600;

/// Errors raised while provisioning a database.
///
/// Every variant is reachable only from the per-database download helpers.
/// [`ensure_databases`] absorbs them all -- see the module-level non-fatal
/// contract.
#[derive(Debug, thiserror::Error)]
pub enum GeoIpDownloadError {
    /// The HTTP request failed at the transport level.
    #[error("HTTP request failed: {0}")]
    Http(#[from] HttpError),

    /// The HTTP client could not be constructed.
    #[error("HTTP client build failed: {0}")]
    HttpClient(#[from] HttpClientError),

    /// The server answered, but not with a database.
    #[error("download of {url} returned HTTP {status}")]
    UnexpectedStatus { url: String, status: u16 },

    /// A filesystem operation failed.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// The blocking decompress/extract task did not complete.
    #[error("decompression task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// The provider needs a credential that was not configured.
    #[error("provider {provider} requires {field} but it was not configured")]
    MissingCredential {
        /// Provider that demanded the credential.
        provider: &'static str,
        /// Config field the operator has to populate.
        field: &'static str,
    },

    /// The downloaded archive did not contain the expected member.
    #[error("{member} not found in the downloaded archive")]
    ArchiveMemberMissing {
        /// File name expected inside the archive.
        member: &'static str,
    },

    /// The provider offers no database of the requested kind.
    #[error("no {kind} database available for provider {provider}")]
    NoDatabases {
        /// Provider that was asked.
        provider: String,
        /// Database kind: `city` or `asn`.
        kind: &'static str,
    },
}

/// Resolved database paths. Either or both may be `None` -- the provider may
/// not offer that kind, or the download may have failed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DatabasePaths {
    /// City-level MMDB, when one is available.
    pub city: Option<PathBuf>,
    /// ASN MMDB, when one is available.
    pub asn: Option<PathBuf>,
}

/// Which database a call is resolving. Keeps the two near-identical resolve
/// arms as one code path instead of two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    City,
    Asn,
}

impl Kind {
    /// Label used in error messages and log fields.
    const fn label(self) -> &'static str {
        match self {
            Self::City => "city",
            Self::Asn => "asn",
        }
    }
}

/// Ensure the configured GeoIP databases are on disk, downloading when the
/// local copy is missing or stale.
///
/// Resolution order:
/// 1. `enabled: false` -- nothing is resolved.
/// 2. Either explicit path set -- those paths are returned verbatim, unchecked.
/// 3. `auto_download.enabled: false` -- only files that already exist.
/// 4. Otherwise: fresh local file, else download, else stale local file.
///
/// # Errors
///
/// Returns no error today: download failures are absorbed and reported as
/// `None` paths. The `Result` reserves room for a hard failure without a
/// breaking signature change.
pub async fn ensure_databases(config: &GeoIpConfig) -> Result<DatabasePaths, GeoIpDownloadError> {
    if !config.enabled {
        debug!("GeoIP provisioning disabled by config");
        return Ok(DatabasePaths::default());
    }

    // Explicit paths bypass the provider entirely, Custom or not. They are
    // returned unchecked: the operator asserted the files exist.
    if config.provider == GeoIpProvider::Custom
        || config.city_db_path.is_some()
        || config.asn_db_path.is_some()
    {
        return Ok(DatabasePaths {
            city: config.city_db_path.clone(),
            asn: config.asn_db_path.clone(),
        });
    }

    let auto = &config.auto_download;
    let (city_file, asn_file) = provider_filenames(config.provider);
    let city_path = city_file.map(|f| auto.data_dir.join(f));
    let asn_path = asn_file.map(|f| auto.data_dir.join(f));

    if !auto.enabled {
        return Ok(DatabasePaths {
            city: city_path.filter(|p| p.exists()),
            asn: asn_path.filter(|p| p.exists()),
        });
    }

    let max_age_secs = u64::from(auto.max_age_days) * SECS_PER_DAY;

    // Sequential, not joined: two concurrent multi-hundred-megabyte transfers
    // would compete for the same link and the same disk.
    Ok(DatabasePaths {
        city: resolve(Kind::City, city_path.as_deref(), config, max_age_secs).await,
        asn: resolve(Kind::Asn, asn_path.as_deref(), config, max_age_secs).await,
    })
}

/// Resolve one database: keep a fresh file, else download, else fall back to
/// whatever stale copy is on disk.
async fn resolve(
    kind: Kind,
    path: Option<&Path>,
    config: &GeoIpConfig,
    max_age_secs: u64,
) -> Option<PathBuf> {
    let path = path?;

    if is_fresh(path, max_age_secs) {
        debug!(kind = kind.label(), path = %path.display(), "GeoIP database is fresh");
        return Some(path.to_path_buf());
    }

    match download(kind, config).await {
        Ok(downloaded) => Some(downloaded),
        Err(e) => {
            warn!(
                kind = kind.label(),
                error = %e,
                provider = ?config.provider,
                "GeoIP database download failed"
            );
            // A stale database still answers most lookups correctly, so it
            // beats disabling enrichment outright.
            if path.exists() {
                warn!(kind = kind.label(), path = %path.display(), "using stale GeoIP database");
                Some(path.to_path_buf())
            } else {
                None
            }
        }
    }
}

/// File name each provider writes for each database kind. `None` means the
/// provider does not publish that kind.
fn provider_filenames(provider: GeoIpProvider) -> (Option<&'static str>, Option<&'static str>) {
    match provider {
        GeoIpProvider::DbIpLite => (Some("dbip-city-lite.mmdb"), Some("dbip-asn-lite.mmdb")),
        GeoIpProvider::MaxMindGeoLite2 => (Some("GeoLite2-City.mmdb"), Some("GeoLite2-ASN.mmdb")),
        GeoIpProvider::IpLocate => (None, Some("iplocate-asn.mmdb")),
        GeoIpProvider::IpInfoLite => (Some("ipinfo-lite.mmdb"), None),
        GeoIpProvider::Sapics => (None, Some("sapics-asn-country.mmdb")),
        GeoIpProvider::Custom => (None, None),
    }
}

/// Whether `path` exists and was modified within `max_age_secs`.
fn is_fresh(path: &Path, max_age_secs: u64) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    // A future mtime makes duration_since fail; treat that as stale rather
    // than trusting a clock that has gone backwards.
    let Ok(age) = SystemTime::now().duration_since(modified) else {
        return false;
    };
    age.as_secs() < max_age_secs
}

/// Build and run the transfer for one database.
async fn download(kind: Kind, config: &GeoIpConfig) -> Result<PathBuf, GeoIpDownloadError> {
    plan(kind, config)?.run().await
}

/// Map (provider, kind) onto a concrete transfer.
///
/// Errors here are configuration faults -- a missing credential or a provider
/// that does not publish this kind -- and cost no network round trip.
fn plan(kind: Kind, config: &GeoIpConfig) -> Result<Transfer, GeoIpDownloadError> {
    let auto = &config.auto_download;
    let dir = &auto.data_dir;
    let unavailable = || GeoIpDownloadError::NoDatabases {
        provider: format!("{:?}", config.provider),
        kind: kind.label(),
    };

    let transfer = match (config.provider, kind) {
        // DB-IP publishes a fresh dated file each month; the current month is
        // the only URL that resolves.
        (GeoIpProvider::DbIpLite, _) => {
            let month = chrono::Utc::now().format("%Y-%m");
            let (slug, file) = match kind {
                Kind::City => ("city", "dbip-city-lite.mmdb"),
                Kind::Asn => ("asn", "dbip-asn-lite.mmdb"),
            };
            Transfer {
                url: format!("https://download.db-ip.com/free/dbip-{slug}-lite-{month}.mmdb.gz"),
                dest: dir.join(file),
                archive: Archive::Gzip,
                credential: Credential::None,
            }
        }

        (GeoIpProvider::MaxMindGeoLite2, _) => {
            let edition = match kind {
                Kind::City => "GeoLite2-City",
                Kind::Asn => "GeoLite2-ASN",
            };
            let member = match kind {
                Kind::City => "GeoLite2-City.mmdb",
                Kind::Asn => "GeoLite2-ASN.mmdb",
            };
            Transfer {
                url: format!(
                    "https://download.maxmind.com/geoip/databases/{edition}/download?suffix=tar.gz"
                ),
                dest: dir.join(member),
                archive: Archive::TarGz { member },
                credential: Credential::Basic {
                    username: require(
                        auto.maxmind_account_id.as_ref(),
                        "MaxMindGeoLite2",
                        "auto_download.maxmind_account_id",
                    )?,
                    password: require(
                        auto.maxmind_license_key.as_ref(),
                        "MaxMindGeoLite2",
                        "auto_download.maxmind_license_key",
                    )?,
                },
            }
        }

        (GeoIpProvider::IpInfoLite, Kind::City) => Transfer {
            url: "https://ipinfo.io/data/ipinfo_lite.mmdb".to_string(),
            dest: dir.join("ipinfo-lite.mmdb"),
            archive: Archive::Raw,
            credential: Credential::QueryToken {
                name: "token",
                value: require(
                    auto.ipinfo_token.as_ref(),
                    "IpInfoLite",
                    "auto_download.ipinfo_token",
                )?,
            },
        },

        (GeoIpProvider::IpLocate, Kind::Asn) => Transfer {
            url: "https://github.com/sapics/ip-location-db/raw/main/dbip-asn/dbip-asn.mmdb"
                .to_string(),
            dest: dir.join("iplocate-asn.mmdb"),
            archive: Archive::Raw,
            credential: Credential::None,
        },

        (GeoIpProvider::Sapics, Kind::Asn) => Transfer {
            url: "https://github.com/sapics/ip-location-db/raw/main/geo-whois-asn-country/geo-whois-asn-country.mmdb"
                .to_string(),
            dest: dir.join("sapics-asn-country.mmdb"),
            archive: Archive::Raw,
            credential: Credential::None,
        },

        // Providers that publish only one of the two kinds, plus Custom, which
        // publishes neither -- its paths are supplied by the operator.
        (GeoIpProvider::IpInfoLite, Kind::Asn)
        | (GeoIpProvider::IpLocate | GeoIpProvider::Sapics, Kind::City)
        | (GeoIpProvider::Custom, _) => return Err(unavailable()),
    };

    Ok(transfer)
}

/// Fetch a required credential, naming the config field when it is absent.
fn require(
    value: Option<&SensitiveString>,
    provider: &'static str,
    field: &'static str,
) -> Result<SensitiveString, GeoIpDownloadError> {
    value
        .cloned()
        .ok_or(GeoIpDownloadError::MissingCredential { provider, field })
}

#[cfg(test)]
mod tests;
