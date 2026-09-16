// Project:   scalo
// File:      src/geoip_download/fetch.rs
// Purpose:   Streaming download + decompression for GeoIP MMDB files
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Transfer plumbing behind [`ensure_databases`](super::ensure_databases).
//!
//! The body streams to a sibling temp file rather than into memory: an MMDB
//! city database is hundreds of megabytes, and a memory-capped pod cannot
//! afford to hold the compressed and decompressed copies at once.
//!
//! Decompression and tar extraction run on
//! [`spawn_blocking`](tokio::task::spawn_blocking) -- both are synchronous
//! CPU-plus-disk work and would otherwise stall a runtime worker for the
//! length of the file.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use reqwest::header::AUTHORIZATION;
use tracing::info;

use super::{DOWNLOAD_TIMEOUT_SECS, GeoIpDownloadError};
use crate::http_client::signer::basic_auth_value;
use crate::http_client::{HttpClient, HttpClientConfig, HttpError, RequestSigner, SignError};
use crate::sensitive::SensitiveString;

/// Extension of the in-flight transfer file, a sibling of the destination so
/// the final rename stays on one filesystem and is therefore atomic.
const PART_EXT: &str = "part";

/// Extension of the fully-materialised file awaiting its rename.
const STAGE_EXT: &str = "staged";

/// How the downloaded bytes are packaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Archive {
    /// The body is the MMDB file itself.
    Raw,
    /// The body is a gzip stream wrapping the MMDB file.
    Gzip,
    /// The body is a gzip-compressed tar carrying `member` somewhere inside.
    TarGz { member: &'static str },
}

/// Credential attached to the request.
///
/// `Debug` is hand-written: a derived one would print the secret into any
/// error report or trace that formats a request plan.
#[derive(Clone)]
pub(super) enum Credential {
    /// Anonymous download.
    None,
    /// HTTP basic auth (MaxMind account id + licence key).
    Basic {
        username: SensitiveString,
        password: SensitiveString,
    },
    /// Token carried as a query parameter (IPinfo).
    QueryToken {
        name: &'static str,
        value: SensitiveString,
    },
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::None => "None",
            Self::Basic { .. } => "Basic(***REDACTED***)",
            Self::QueryToken { .. } => "QueryToken(***REDACTED***)",
        };
        f.write_str(kind)
    }
}

/// Attaching the credential is a [`RequestSigner`], so the provider auth rides
/// the same hook every other signed call in the codebase uses.
///
/// The token goes on the built URL rather than being formatted into the URL
/// string, so the URL the caller logs never carries it.
impl RequestSigner for Credential {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        match self {
            Self::None => {}
            Self::Basic { username, password } => {
                let value = basic_auth_value(username.expose(), Some(password.expose()))?;
                request.headers_mut().insert(AUTHORIZATION, value);
            }
            Self::QueryToken { name, value } => {
                request
                    .url_mut()
                    .query_pairs_mut()
                    .append_pair(name, value.expose());
            }
        }
        Ok(())
    }
}

/// One database transfer: where from, where to, and how it is packaged.
#[derive(Debug)]
pub(super) struct Transfer {
    pub(super) url: String,
    pub(super) dest: PathBuf,
    pub(super) archive: Archive,
    pub(super) credential: Credential,
}

impl Transfer {
    /// Fetch, materialise and atomically move the database into place.
    ///
    /// Returns the destination path on success.
    pub(super) async fn run(self) -> Result<PathBuf, GeoIpDownloadError> {
        if let Some(parent) = self.dest.parent() {
            fs::create_dir_all(parent)?;
        }

        let part = with_extension(&self.dest, PART_EXT);
        info!(
            url = %self.url,
            dest = %self.dest.display(),
            archive = ?self.archive,
            "downloading GeoIP database"
        );

        // A failed transfer must not leave a partial file that a later run
        // mistakes for a complete one.
        let bytes = match self.stream_to(&part).await {
            Ok(bytes) => bytes,
            Err(e) => {
                let _ = fs::remove_file(&part);
                return Err(e);
            }
        };

        let dest = self.dest.clone();
        let archive = self.archive;
        let staged = with_extension(&dest, STAGE_EXT);
        let final_size = tokio::task::spawn_blocking(move || {
            let result = materialise(&part, &staged, &dest, archive);
            let _ = fs::remove_file(&part);
            if result.is_err() {
                let _ = fs::remove_file(&staged);
            }
            result
        })
        .await??;

        info!(
            dest = %self.dest.display(),
            downloaded_bytes = bytes,
            database_bytes = final_size,
            "GeoIP database ready"
        );
        Ok(self.dest)
    }

    /// Stream the response body to `part`, returning the byte count.
    async fn stream_to(&self, part: &Path) -> Result<u64, GeoIpDownloadError> {
        // Built per download, so the steady-state path where both databases are
        // fresh constructs no client at all.
        let mut config = HttpClientConfig::from_cascade();
        // The cascade timeout is sized for API calls; an MMDB transfer needs
        // minutes, so this one is not operator-tunable.
        config.timeout_secs = DOWNLOAD_TIMEOUT_SECS;
        config.user_agent = Some(format!("scalo/{}", crate::VERSION));
        let client = HttpClient::new(config)?;

        let mut response = client.get_signed(&self.url, &self.credential).await?;

        // HttpClient hands back a persistent 4xx/5xx as Ok so the caller can
        // inspect it, so the status check is ours to make.
        if !response.status().is_success() {
            return Err(GeoIpDownloadError::UnexpectedStatus {
                url: self.url.clone(),
                status: response.status().as_u16(),
            });
        }

        let mut file = fs::File::create(part)?;
        let mut written = 0u64;
        while let Some(chunk) = response.chunk().await.map_err(HttpError::from)? {
            io::Write::write_all(&mut file, &chunk)?;
            written += chunk.len() as u64;
        }
        io::Write::flush(&mut file)?;
        Ok(written)
    }
}

/// Turn the transferred bytes into the destination file. Blocking: gzip and
/// tar decode are synchronous and this runs on the blocking pool.
fn materialise(
    part: &Path,
    staged: &Path,
    dest: &Path,
    archive: Archive,
) -> Result<u64, GeoIpDownloadError> {
    let source = fs::File::open(part)?;

    match archive {
        Archive::Raw => {
            fs::rename(part, staged)?;
        }
        Archive::Gzip => {
            let mut decoder = GzDecoder::new(io::BufReader::new(source));
            let mut out = io::BufWriter::new(fs::File::create(staged)?);
            io::copy(&mut decoder, &mut out)?;
            io::Write::flush(&mut out)?;
        }
        Archive::TarGz { member } => {
            extract_member(source, staged, member)?;
        }
    }

    let size = fs::metadata(staged)?.len();
    fs::rename(staged, dest)?;
    Ok(size)
}

/// Extract a single named member from a gzip-compressed tar.
///
/// The archives carry the file under a dated directory
/// (`GeoLite2-City_20241231/GeoLite2-City.mmdb`), so the match is on the file
/// name rather than the full path.
fn extract_member(
    source: fs::File,
    staged: &Path,
    member: &'static str,
) -> Result<(), GeoIpDownloadError> {
    let decoder = GzDecoder::new(io::BufReader::new(source));
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let is_match = entry.path()?.file_name().is_some_and(|name| name == member);
        if is_match {
            let mut out = io::BufWriter::new(fs::File::create(staged)?);
            io::copy(&mut entry, &mut out)?;
            io::Write::flush(&mut out)?;
            return Ok(());
        }
    }

    Err(GeoIpDownloadError::ArchiveMemberMissing { member })
}

/// Append an extension rather than replacing one: `foo.mmdb` becomes
/// `foo.mmdb.part`, so two providers writing different databases into the same
/// directory never collide on a temp name.
fn with_extension(path: &Path, extension: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".");
    name.push(extension);
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_names_append_rather_than_replace() {
        let dest = Path::new("/var/lib/geoip/dbip-city-lite.mmdb");
        assert_eq!(
            with_extension(dest, PART_EXT),
            PathBuf::from("/var/lib/geoip/dbip-city-lite.mmdb.part")
        );
        assert_eq!(
            with_extension(dest, STAGE_EXT),
            PathBuf::from("/var/lib/geoip/dbip-city-lite.mmdb.staged")
        );
    }

    #[test]
    fn credential_debug_never_shows_the_secret() {
        let basic = Credential::Basic {
            username: "account-1234".into(),
            password: "licence-abcd".into(),
        };
        let token = Credential::QueryToken {
            name: "token",
            value: "token-wxyz".into(),
        };
        assert_eq!(format!("{basic:?}"), "Basic(***REDACTED***)");
        assert_eq!(format!("{token:?}"), "QueryToken(***REDACTED***)");
        assert_eq!(format!("{:?}", Credential::None), "None");
    }

    #[test]
    fn transfer_debug_never_shows_the_secret() {
        let transfer = Transfer {
            url: "https://example.invalid/db.mmdb".into(),
            dest: PathBuf::from("/tmp/db.mmdb"),
            archive: Archive::Raw,
            credential: Credential::QueryToken {
                name: "token",
                value: "token-wxyz".into(),
            },
        };
        let rendered = format!("{transfer:?}");
        assert!(!rendered.contains("token-wxyz"), "{rendered}");
        assert!(rendered.contains("REDACTED"), "{rendered}");
    }

    /// Each provider's credential lands where that provider wants it: basic
    /// auth in a sensitive header, a token as an encoded query parameter
    /// alongside the parameters the URL already carries, and nothing at all
    /// when the provider needs nothing.
    #[tokio::test]
    async fn the_credential_signs_where_the_provider_wants_it() {
        let url = "https://download.example/db.mmdb?suffix=tar.gz";
        let mut request = reqwest::Request::new(reqwest::Method::GET, url.parse().unwrap());
        Credential::Basic {
            username: "account-1234".into(),
            password: "licence-key".into(),
        }
        .sign(&mut request)
        .await
        .unwrap();
        let authorization = request.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(
            authorization.to_str().unwrap(),
            "Basic YWNjb3VudC0xMjM0OmxpY2VuY2Uta2V5"
        );
        assert!(authorization.is_sensitive());

        let mut request = reqwest::Request::new(reqwest::Method::GET, url.parse().unwrap());
        Credential::QueryToken {
            name: "token",
            value: "token-wxyz".into(),
        }
        .sign(&mut request)
        .await
        .unwrap();
        assert_eq!(
            request.url().query(),
            Some("suffix=tar.gz&token=token-wxyz"),
            "appended to the query the provider already needs"
        );

        let mut request = reqwest::Request::new(reqwest::Method::GET, url.parse().unwrap());
        Credential::None.sign(&mut request).await.unwrap();
        assert!(request.headers().is_empty());
        assert_eq!(request.url().query(), Some("suffix=tar.gz"));
    }

    #[test]
    fn materialise_gzip_writes_the_decompressed_file() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("db.mmdb");
        let part = with_extension(&dest, PART_EXT);
        let staged = with_extension(&dest, STAGE_EXT);

        let payload = b"not really an mmdb, but it round-trips";
        let mut encoder = flate2::write::GzEncoder::new(
            fs::File::create(&part).unwrap(),
            flate2::Compression::fast(),
        );
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap();

        let size = materialise(&part, &staged, &dest, Archive::Gzip).unwrap();
        assert_eq!(usize::try_from(size).unwrap(), payload.len());
        assert_eq!(fs::read(&dest).unwrap(), payload);
        assert!(!staged.exists(), "staged file must be renamed away");
    }

    #[test]
    fn materialise_raw_renames_the_body_into_place() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("db.mmdb");
        let part = with_extension(&dest, PART_EXT);
        let staged = with_extension(&dest, STAGE_EXT);

        fs::write(&part, b"raw body").unwrap();
        let size = materialise(&part, &staged, &dest, Archive::Raw).unwrap();

        assert_eq!(size, 8);
        assert_eq!(fs::read(&dest).unwrap(), b"raw body");
    }

    #[test]
    fn materialise_tar_gz_extracts_the_named_member() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("GeoLite2-City.mmdb");
        let part = with_extension(&dest, PART_EXT);
        let staged = with_extension(&dest, STAGE_EXT);

        // Mirror the real layout: the member sits under a dated directory.
        let payload = b"city database bytes";
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            fs::File::create(&part).unwrap(),
            flate2::Compression::fast(),
        ));
        let mut header = tar::Header::new_gnu();
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(
                &mut header,
                "GeoLite2-City_20241231/GeoLite2-City.mmdb",
                &payload[..],
            )
            .unwrap();
        builder
            .into_inner()
            .unwrap()
            .finish()
            .unwrap()
            .flush()
            .unwrap();

        let size = materialise(
            &part,
            &staged,
            &dest,
            Archive::TarGz {
                member: "GeoLite2-City.mmdb",
            },
        )
        .unwrap();

        assert_eq!(usize::try_from(size).unwrap(), payload.len());
        assert_eq!(fs::read(&dest).unwrap(), payload);
    }

    #[test]
    fn materialise_tar_gz_reports_a_missing_member() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("GeoLite2-ASN.mmdb");
        let part = with_extension(&dest, PART_EXT);
        let staged = with_extension(&dest, STAGE_EXT);

        let builder = tar::Builder::new(flate2::write::GzEncoder::new(
            fs::File::create(&part).unwrap(),
            flate2::Compression::fast(),
        ));
        builder.into_inner().unwrap().finish().unwrap();

        let err = materialise(
            &part,
            &staged,
            &dest,
            Archive::TarGz {
                member: "GeoLite2-ASN.mmdb",
            },
        )
        .unwrap_err();

        assert!(
            matches!(err, GeoIpDownloadError::ArchiveMemberMissing { member } if member == "GeoLite2-ASN.mmdb"),
            "{err:?}"
        );
        assert!(!dest.exists());
    }
}
