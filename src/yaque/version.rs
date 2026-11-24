// Copied from yaque 0.6.6, src/version.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! The format version stamped in a queue, so a build never reads a queue
//! written in a format it does not know.

use std::io;
use std::path::Path;

use semver::{Version, VersionReq};

/// The version stamped in `version/contents`. It names the on-disk format, so
/// it never follows scalo's release. 0.7 puts a codec tag, a CRC32C and the
/// write time in front of every entry.
pub(crate) const QUEUE_FORMAT_VERSION: &str = "0.7.0";

/// The stamped versions a queue may carry and still open.
pub(crate) const QUEUE_FORMAT_ACCEPTS: &str = "0.7.*";

/// Stamps the format into a queue that has none, or checks the one it has.
/// The caller holds the queue's locks, so no other open races the stamp.
///
/// # Errors
///
/// `InvalidData` naming the file when it holds no version, `Unsupported` for
/// one outside [`QUEUE_FORMAT_ACCEPTS`], or the read's or write's error.
pub(crate) fn check_queue_version(base: &Path) -> io::Result<()> {
    let dir = base.join("version");
    std::fs::create_dir_all(&dir)?;
    let file = dir.join("contents");
    let contents = match std::fs::read(&file) {
        Ok(contents) => contents,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    if contents.is_empty() {
        return std::fs::write(&file, format!("{QUEUE_FORMAT_VERSION}\n"));
    }
    let text = String::from_utf8_lossy(&contents);
    let shown: String = text.trim().chars().take(40).collect();
    let invalid = |why: String| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} {why}", file.display()),
        )
    };
    let named = Version::parse(text.trim())
        .map_err(|e| invalid(format!("does not hold a version ({e}): {shown:?}")))?;
    let accepted = VersionReq::parse(QUEUE_FORMAT_ACCEPTS)
        .map_err(|e| invalid(format!("cannot be checked: {e}")))?;
    if !accepted.matches(&named) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "{} names spool format {named}, and this build reads {QUEUE_FORMAT_ACCEPTS}. \
                 Drain it with the release that wrote it, or remove it",
                file.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh queue is stamped with a version it accepts. Another format is
    /// `Unsupported` and a stamp that is no version `InvalidData`, each naming
    /// the file, never a panic.
    #[test]
    fn a_stamp_is_written_once_and_checked_after() {
        use io::ErrorKind::{InvalidData, Unsupported};
        let rows: [(&str, Option<&[u8]>, Option<io::ErrorKind>); 6] = [
            ("fresh", None, None),
            ("this format", Some(b"0.7.0\n"), None),
            ("the format before", Some(b"0.6.6\n"), Some(Unsupported)),
            ("an older format", Some(b"0.5.2\n"), Some(Unsupported)),
            ("not a version", Some(b"not a version"), Some(InvalidData)),
            ("only whitespace", Some(b"\n"), Some(InvalidData)),
        ];
        for (name, stamp, refused) in rows {
            let dir = tempfile::tempdir().unwrap();
            if let Some(stamp) = stamp {
                std::fs::create_dir_all(dir.path().join("version")).unwrap();
                std::fs::write(dir.path().join("version").join("contents"), stamp).unwrap();
            }
            match check_queue_version(dir.path()) {
                Ok(()) => assert_eq!(refused, None, "{name}: opened"),
                Err(e) => {
                    assert_eq!(Some(e.kind()), refused, "{name}: {e}");
                    assert!(e.to_string().contains("contents"), "{name}: {e}");
                }
            }
        }
        let dir = tempfile::tempdir().unwrap();
        check_queue_version(dir.path()).unwrap();
        let stamped = std::fs::read_to_string(dir.path().join("version").join("contents")).unwrap();
        assert_eq!(stamped.trim(), QUEUE_FORMAT_VERSION);
        check_queue_version(dir.path()).expect("a stamped queue reopens");
    }
}
