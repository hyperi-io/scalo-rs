// Copied from yaque 0.6.6, src/version.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! The format version stamped in a queue, so a build never reads a queue
//! written in a format it does not know.

use std::io;
use std::path::Path;

use semver::{Version, VersionReq};

use super::queue::{scan, segment_filename, segments};
use super::state::{Persistence, QueueState, RECV_METADATA};

/// The version stamped in `version/contents`. It names the on-disk format, so
/// it never follows scalo's release. 0.7 puts a codec tag, a CRC32C and the
/// write time in front of every entry.
pub(crate) const QUEUE_FORMAT_VERSION: &str = "0.7.0";

/// The stamped versions a queue may carry and still open.
pub(crate) const QUEUE_FORMAT_ACCEPTS: &str = "0.7.*";

/// The lock file a process of an earlier format may still hold, read before
/// this open rewrites it. That format writes a `token=` line into a lock file
/// it creates, and removes the file when it stops.
pub(crate) fn earlier_owner(base: &Path) -> Option<String> {
    ["send.lock", "recv.lock"].into_iter().find_map(|name| {
        let text = std::fs::read_to_string(base.join(name)).ok()?;
        text.lines().any(|line| line.starts_with("token=")).then(|| {
            let pid = text
                .lines()
                .find_map(|line| line.strip_prefix("pid="))
                .unwrap_or("unknown");
            format!("{name} names pid {pid}, a process of that release that is running or was killed")
        })
    })
}

/// Stamps the format into a queue that has none, or checks the one it has.
/// A queue in an earlier format that holds no unread entry is cleared and
/// stamped, since nothing in it is lost. The caller holds the queue's locks,
/// so no other open races the stamp.
///
/// # Errors
///
/// `InvalidData` naming the file when it holds no version. `Unsupported` for a
/// later format, and for an earlier one that still holds entries or whose
/// owner may still run. Otherwise the read's, removal's or write's error.
pub(crate) fn check_queue_version(base: &Path, earlier_owner: Option<&str>) -> io::Result<()> {
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
    if accepted.matches(&named) {
        return Ok(());
    }
    let current = Version::parse(QUEUE_FORMAT_VERSION)
        .map_err(|e| invalid(format!("cannot be checked: {e}")))?;
    let refused = |why: String| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("{} names spool format {named}, {why}", file.display()),
        )
    };
    if named > current {
        return Err(refused(
            "which a later release wrote. Open it with that release".to_string(),
        ));
    }
    if let Some(owner) = earlier_owner {
        return Err(refused(format!(
            "and {owner}. Stop that process, or remove the lock file once it is gone"
        )));
    }
    match unread(base)? {
        Some((0, _)) => {
            clear(base)?;
            std::fs::write(&file, format!("{QUEUE_FORMAT_VERSION}\n"))?;
            tracing::info!(
                path = %base.display(),
                from = %named,
                to = QUEUE_FORMAT_VERSION,
                "cleared a drained spool written in an earlier format, and stamped it with this one"
            );
            Ok(())
        }
        Some((entries, bytes)) => Err(refused(format!(
            "which this build does not read, and holds {entries} unread records ({bytes} bytes). \
             Drain them with the release that wrote it, or remove the directory, losing them"
        ))),
        None => Err(refused(format!(
            "which this build does not read, and a damaged header hides how many unread records \
             it holds. Remove the directory, losing its {} bytes of segments",
            segment_bytes(base)?
        ))),
    }
}

/// The unread entries and their bytes from the saved read position, walking
/// only the headers, which this format shares with the earlier one. `None`
/// when a header is damaged, since nothing past it can be counted.
fn unread(base: &Path) -> io::Result<Option<(u64, u64)>> {
    let numbers = segments(base)?;
    let Some(&first) = numbers.first() else {
        return Ok(Some((0, 0)));
    };
    // A position that names no segment here, or lies past its end, counts from the first segment.
    let from = Persistence::open(base)
        .ok()
        .and_then(|(_, saved)| saved)
        .filter(|saved| {
            numbers.binary_search(&saved.segment).is_ok()
                && std::fs::metadata(segment_filename(base, saved.segment))
                    .is_ok_and(|meta| saved.position <= meta.len())
        })
        .unwrap_or(QueueState {
            segment: first,
            position: 0,
        });
    let (mut entries, mut bytes) = (0, 0);
    for number in numbers.into_iter().filter(|number| *number >= from.segment) {
        let start = if number == from.segment {
            from.position
        } else {
            0
        };
        let found = scan(&segment_filename(base, number), start, None)?;
        if found.damaged {
            return Ok(None);
        }
        entries += found.entries;
        bytes += found.bytes;
    }
    Ok(Some((entries, bytes)))
}

/// Removes a drained queue's segments and its saved position, so it starts
/// again from segment 0.
fn clear(base: &Path) -> io::Result<()> {
    let names = segments(base)?
        .into_iter()
        .map(|number| segment_filename(base, number))
        .chain([
            base.join(RECV_METADATA),
            base.join(format!("{RECV_METADATA}.tmp")),
        ]);
    for path in names {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// The bytes a queue's segment files hold.
fn segment_bytes(base: &Path) -> io::Result<u64> {
    let mut total = 0;
    for number in segments(base)? {
        total += std::fs::metadata(segment_filename(base, number))?.len();
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh queue is stamped with a version it accepts, and an empty one
    /// stamped in an earlier format is restamped. A later format is
    /// `Unsupported` and a stamp that is no version `InvalidData`, each naming
    /// the file, never a panic.
    #[test]
    fn a_stamp_is_written_once_and_checked_after() {
        use io::ErrorKind::{InvalidData, Unsupported};
        let rows: [(&str, Option<&[u8]>, Option<io::ErrorKind>); 7] = [
            ("fresh", None, None),
            ("this format", Some(b"0.7.0\n"), None),
            ("the format before, empty", Some(b"0.6.6\n"), None),
            ("an older format, empty", Some(b"0.5.2\n"), None),
            ("a later format", Some(b"0.8.0\n"), Some(Unsupported)),
            ("not a version", Some(b"not a version"), Some(InvalidData)),
            ("only whitespace", Some(b"\n"), Some(InvalidData)),
        ];
        for (name, stamp, refused) in rows {
            let dir = tempfile::tempdir().unwrap();
            if let Some(stamp) = stamp {
                std::fs::create_dir_all(dir.path().join("version")).unwrap();
                std::fs::write(dir.path().join("version").join("contents"), stamp).unwrap();
            }
            match check_queue_version(dir.path(), None) {
                Ok(()) => {
                    assert_eq!(refused, None, "{name}: opened");
                    let stamped =
                        std::fs::read_to_string(dir.path().join("version").join("contents"))
                            .unwrap();
                    assert_eq!(stamped.trim(), QUEUE_FORMAT_VERSION, "{name}");
                }
                Err(e) => {
                    assert_eq!(Some(e.kind()), refused, "{name}: {e}");
                    assert!(e.to_string().contains("contents"), "{name}: {e}");
                }
            }
        }
        let dir = tempfile::tempdir().unwrap();
        check_queue_version(dir.path(), None).unwrap();
        check_queue_version(dir.path(), None).expect("a stamped queue reopens");
    }

    /// An entry as the earlier format wrote it: the same header, then the payload.
    fn entry(payload: &[u8]) -> Vec<u8> {
        let mut bytes = super::super::header::Header::for_len(payload.len())
            .unwrap()
            .encode()
            .to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    /// One queue in an earlier format, and what its open does.
    struct Case {
        name: &'static str,
        stamp: &'static str,
        segment: Vec<u8>,
        /// The saved read position in segment 0, or none saved.
        read_to: Option<u64>,
        lock: Option<&'static str>,
        /// What the refusal names, or `None` when the queue is cleared and opens.
        refused: Option<&'static str>,
    }

    /// A 0.6 queue opens once it holds no unread entry: it is cleared and
    /// stamped. One with unread entries, a damaged header past the position,
    /// or a lock file its release may still hold is refused and left as it
    /// was, and so is a later format.
    #[test]
    fn an_earlier_format_opens_only_once_drained() {
        let two = [entry(b"first"), entry(b"second")].concat();
        let end = Some(two.len() as u64);
        let case = |name, stamp, segment: &[u8], read_to, lock, refused| Case {
            name,
            stamp,
            segment: segment.to_vec(),
            read_to,
            lock,
            refused,
        };
        let damaged = [entry(b"first"), b"Asbt".to_vec()].concat();
        let cases = [
            case("read to its end", "0.6.6", &two, end, None, None),
            case("an empty segment", "0.6.6", b"", None, None, None),
            case(
                "this format's lock left",
                "0.6.6",
                &two,
                end,
                Some("pid=1\n"),
                None,
            ),
            case(
                "unread entries",
                "0.6.6",
                &two,
                Some(0),
                None,
                Some("2 unread records (11 bytes)"),
            ),
            case(
                "no saved position",
                "0.6.6",
                &two,
                None,
                None,
                Some("2 unread records"),
            ),
            case(
                "a damaged header",
                "0.6.6",
                &damaged,
                Some(0),
                None,
                Some("damaged header"),
            ),
            case(
                "an earlier owner's lock",
                "0.6.6",
                &two,
                end,
                Some("pid=4242\ntoken=7\n"),
                Some("pid 4242"),
            ),
            case(
                "a later format",
                "0.8.0",
                &two,
                end,
                None,
                Some("later release"),
            ),
        ];
        for Case {
            name,
            stamp,
            segment,
            read_to,
            lock,
            refused,
        } in cases
        {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path();
            std::fs::create_dir_all(base.join("version")).unwrap();
            std::fs::write(base.join("version").join("contents"), format!("{stamp}\n")).unwrap();
            std::fs::write(segment_filename(base, 0), &segment).unwrap();
            if let Some(position) = read_to {
                let mut saved = [0_u8; 16];
                saved[8..].copy_from_slice(&position.to_be_bytes());
                std::fs::write(base.join(RECV_METADATA), saved).unwrap();
            }
            if let Some(lock) = lock {
                std::fs::write(base.join("send.lock"), lock).unwrap();
            }
            let opened = check_queue_version(base, earlier_owner(base).as_deref());
            let stamped = std::fs::read_to_string(base.join("version").join("contents")).unwrap();
            match (opened, refused) {
                (Ok(()), None) => {
                    assert_eq!(stamped.trim(), QUEUE_FORMAT_VERSION, "{name}");
                    assert!(
                        segments(base).unwrap().is_empty(),
                        "{name}: segments cleared"
                    );
                    assert!(
                        !base.join(RECV_METADATA).exists(),
                        "{name}: position cleared"
                    );
                }
                (Err(e), Some(named)) => {
                    assert_eq!(e.kind(), io::ErrorKind::Unsupported, "{name}: {e}");
                    assert!(e.to_string().contains(named), "{name}: {e}");
                    let kept = std::fs::read(segment_filename(base, 0)).unwrap();
                    assert_eq!(kept, segment, "{name}: segment kept");
                    assert_eq!(stamped.trim(), stamp, "{name}: stamp kept");
                }
                (opened, refused) => panic!("{name}: {opened:?}, wanted refusal {refused:?}"),
            }
        }
    }
}
