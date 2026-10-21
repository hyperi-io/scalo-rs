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
    /// Fetch, materialise, check and atomically move the database into place,
    /// refusing a body or a database over `max_bytes`.
    ///
    /// Returns the destination path on success. On failure the destination is
    /// left as it was.
    pub(super) async fn run(self, max_bytes: u64) -> Result<PathBuf, GeoIpDownloadError> {
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
        let bytes = match self.stream_to(&part, max_bytes).await {
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
            let result = materialise(&part, &staged, &dest, archive, max_bytes);
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

    /// Stream the response body to `part`, returning the byte count, and stop
    /// once it passes `max_bytes`.
    async fn stream_to(&self, part: &Path, max_bytes: u64) -> Result<u64, GeoIpDownloadError> {
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
            written += chunk.len() as u64;
            if written > max_bytes {
                return Err(GeoIpDownloadError::TooLarge {
                    what: "download",
                    limit: max_bytes,
                });
            }
            io::Write::write_all(&mut file, &chunk)?;
        }
        io::Write::flush(&mut file)?;
        Ok(written)
    }
}

/// Turn the transferred bytes into the destination file, refusing a database
/// over `max_bytes` or one that does not read as an MMDB. Blocking: gzip and
/// tar decode are synchronous and this runs on the blocking pool.
fn materialise(
    part: &Path,
    staged: &Path,
    dest: &Path,
    archive: Archive,
    max_bytes: u64,
) -> Result<u64, GeoIpDownloadError> {
    let source = fs::File::open(part)?;

    match archive {
        Archive::Raw => {
            fs::rename(part, staged)?;
        }
        Archive::Gzip => {
            let decoder = GzDecoder::new(io::BufReader::new(source));
            write_capped(decoder, staged, max_bytes)?;
        }
        Archive::TarGz { member } => {
            extract_member(source, staged, member, max_bytes)?;
        }
    }

    let size = fs::metadata(staged)?.len();
    if size > max_bytes {
        return Err(GeoIpDownloadError::TooLarge {
            what: "database",
            limit: max_bytes,
        });
    }
    super::mmdb::verify(staged)?;
    fs::rename(staged, dest)?;
    Ok(size)
}

/// Copy `reader` to `staged`, stopping once it passes `max_bytes`.
fn write_capped(
    reader: impl io::Read,
    staged: &Path,
    max_bytes: u64,
) -> Result<(), GeoIpDownloadError> {
    let mut out = io::BufWriter::new(fs::File::create(staged)?);
    // One byte past the cap is enough to tell an over-size stream from one at it.
    let copied = io::copy(&mut reader.take(max_bytes.saturating_add(1)), &mut out)?;
    if copied > max_bytes {
        return Err(GeoIpDownloadError::TooLarge {
            what: "database",
            limit: max_bytes,
        });
    }
    io::Write::flush(&mut out)?;
    Ok(())
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
    max_bytes: u64,
) -> Result<(), GeoIpDownloadError> {
    let decoder = GzDecoder::new(io::BufReader::new(source));
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let entry = entry?;
        let is_match = entry.path()?.file_name().is_some_and(|name| name == member);
        if is_match {
            return write_capped(entry, staged, max_bytes);
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

    /// A cap no test database comes near.
    const ROOM: u64 = 1 << 20;

    const MEMBER: &str = "GeoLite2-City.mmdb";

    fn gzip(payload: &[u8], path: &Path) {
        use std::io::Write;

        let mut encoder = flate2::write::GzEncoder::new(
            fs::File::create(path).unwrap(),
            flate2::Compression::fast(),
        );
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap();
    }

    /// A gzip-compressed tar holding `payload` as [`MEMBER`] under a dated
    /// directory, as the real archives lay it out.
    fn tar_gz(payload: &[u8], path: &Path) {
        use std::io::Write;

        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            fs::File::create(path).unwrap(),
            flate2::Compression::fast(),
        ));
        let mut header = tar::Header::new_gnu();
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(
                &mut header,
                format!("GeoLite2-City_20241231/{MEMBER}"),
                payload,
            )
            .unwrap();
        builder
            .into_inner()
            .unwrap()
            .finish()
            .unwrap()
            .flush()
            .unwrap();
    }

    /// Write `payload` to `part` in `archive`'s packaging.
    fn package(archive: Archive, payload: &[u8], part: &Path) {
        match archive {
            Archive::Raw => fs::write(part, payload).unwrap(),
            Archive::Gzip => gzip(payload, part),
            Archive::TarGz { .. } => tar_gz(payload, part),
        }
    }

    const ARCHIVES: [Archive; 3] = [
        Archive::Raw,
        Archive::Gzip,
        Archive::TarGz { member: MEMBER },
    ];

    #[test]
    fn materialise_writes_the_database_in_every_packaging() {
        let database = super::super::mmdb::sample();
        for archive in ARCHIVES {
            let dir = tempfile::tempdir().unwrap();
            let dest = dir.path().join(MEMBER);
            let part = with_extension(&dest, PART_EXT);
            let staged = with_extension(&dest, STAGE_EXT);
            package(archive, &database, &part);

            let size = materialise(&part, &staged, &dest, archive, ROOM).unwrap();
            assert_eq!(
                usize::try_from(size).unwrap(),
                database.len(),
                "{archive:?}"
            );
            assert_eq!(fs::read(&dest).unwrap(), database, "{archive:?}");
            assert!(
                !staged.exists(),
                "{archive:?}: the staged file is renamed away"
            );
        }
    }

    /// A database past the cap is refused, at any packaging, and the database
    /// already in place stays.
    #[test]
    fn materialise_refuses_a_database_past_the_cap() {
        let database = super::super::mmdb::sample();
        let cap = u64::try_from(database.len()).unwrap() - 1;
        for archive in ARCHIVES {
            let dir = tempfile::tempdir().unwrap();
            let dest = dir.path().join(MEMBER);
            let part = with_extension(&dest, PART_EXT);
            let staged = with_extension(&dest, STAGE_EXT);
            fs::write(&dest, b"the working database").unwrap();
            package(archive, &database, &part);

            let err = materialise(&part, &staged, &dest, archive, cap).unwrap_err();
            assert!(
                matches!(err, GeoIpDownloadError::TooLarge { what: "database", limit } if limit == cap),
                "{archive:?}: {err:?}"
            );
            assert_eq!(fs::read(&dest).unwrap(), b"the working database");
        }
        // At the cap exactly, it lands.
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join(MEMBER);
        let part = with_extension(&dest, PART_EXT);
        gzip(&database, &part);
        let at_cap = u64::try_from(database.len()).unwrap();
        let staged = with_extension(&dest, STAGE_EXT);
        assert!(materialise(&part, &staged, &dest, Archive::Gzip, at_cap).is_ok());
    }

    /// An error page or any other file that is not an MMDB never replaces the
    /// database in place.
    #[test]
    fn materialise_refuses_a_file_that_is_not_a_database() {
        for archive in ARCHIVES {
            let dir = tempfile::tempdir().unwrap();
            let dest = dir.path().join(MEMBER);
            let part = with_extension(&dest, PART_EXT);
            let staged = with_extension(&dest, STAGE_EXT);
            fs::write(&dest, b"the working database").unwrap();
            package(archive, b"<html>rate limited</html>", &part);

            let err = materialise(&part, &staged, &dest, archive, ROOM).unwrap_err();
            assert!(
                matches!(err, GeoIpDownloadError::NotMmdb { .. }),
                "{archive:?}: {err:?}"
            );
            assert_eq!(fs::read(&dest).unwrap(), b"the working database");
        }
    }

    /// Serve `body` from a loopback listener, returning its URL.
    async fn serve(body: Vec<u8>) -> String {
        let app = axum::Router::new().route(
            "/db",
            axum::routing::get(move || {
                let body = body.clone();
                async move { body }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/db")
    }

    fn transfer(url: String, dest: PathBuf, archive: Archive) -> Transfer {
        Transfer {
            url,
            dest,
            archive,
            credential: Credential::None,
        }
    }

    /// A body past the cap is abandoned mid-stream and leaves no file behind.
    #[tokio::test]
    async fn a_download_past_the_cap_is_abandoned() {
        let url = serve(vec![0u8; 64 * 1024]).await;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("db.mmdb");

        let err = transfer(url, dest.clone(), Archive::Raw)
            .run(1024)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                GeoIpDownloadError::TooLarge {
                    what: "download",
                    limit: 1024
                }
            ),
            "{err:?}"
        );
        assert!(!dest.exists());
        assert!(
            !with_extension(&dest, PART_EXT).exists(),
            "the part file stays behind"
        );
    }

    /// A well-formed database under the cap lands, and an error page served
    /// with a 200 is refused without touching the database in place.
    #[tokio::test]
    async fn a_download_lands_only_when_it_is_a_database() {
        let database = super::super::mmdb::sample();
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("served.gz");
        gzip(&database, &part);
        let url = serve(fs::read(&part).unwrap()).await;
        let dest = dir.path().join("db.mmdb");
        let landed = transfer(url, dest.clone(), Archive::Gzip)
            .run(ROOM)
            .await
            .unwrap();
        assert_eq!(fs::read(&landed).unwrap(), database);

        let url = serve(b"<html>quota exceeded</html>".to_vec()).await;
        let err = transfer(url, dest.clone(), Archive::Raw)
            .run(ROOM)
            .await
            .unwrap_err();
        assert!(matches!(err, GeoIpDownloadError::NotMmdb { .. }), "{err:?}");
        assert_eq!(
            fs::read(&dest).unwrap(),
            database,
            "the database in place was replaced"
        );
        assert!(!with_extension(&dest, STAGE_EXT).exists());
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
            ROOM,
        )
        .unwrap_err();

        assert!(
            matches!(err, GeoIpDownloadError::ArchiveMemberMissing { member } if member == "GeoLite2-ASN.mmdb"),
            "{err:?}"
        );
        assert!(!dest.exists());
    }
}
