// Project:   scalo
// File:      src/io/rotating.rs
// Purpose:   The live file under NdjsonWriter: open, append, rotate, retain
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The live file under [`super::NdjsonWriter`].
//!
//! Opening cuts a last line with no newline back to the newline before it. An
//! append lands whole or is cut back off. Rotation renames the live file to
//! `<name>.<local YYYYMMDDTHHMMSS>[.N]`, then applies the count and age limits
//! and compresses all but the six newest rotated files with `compress_rotated`.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp, ZonedDateTime};
use tracing::{debug, warn};

use super::config::{FileMode, FileWriterConfig, RotationPeriod};
use crate::compression::Codec;

/// Rotated files left uncompressed when `compress_rotated` names a codec, newest first.
const KEEP_UNCOMPRESSED: usize = 6;

/// Suffix format of a rotated file, in local time.
const STAMP_FORMAT: &str = "%Y%m%dT%H%M%S";

/// Length of a formatted [`STAMP_FORMAT`] for years 1000 to 9999.
const STAMP_LEN: usize = 15;

/// How far back the partial-line scan reads at a time.
const SCAN_CHUNK: u64 = 64 * 1024;

/// Why a rotation ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Trigger {
    /// The time period of the last write has passed.
    Time,
    /// The write would take the file past `max_bytes`.
    Size,
}

impl Trigger {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Time => "time",
            Self::Size => "size",
        }
    }
}

/// What opening does to bytes already in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenMode {
    /// Keep them and write after them.
    Append,
    /// Drop them.
    Truncate,
}

/// The open live file and what the writer tracks about it.
#[derive(Debug)]
pub(super) struct LiveFile {
    file: File,
    len: u64,
    /// Local hour of the last write, for the time trigger.
    period: Option<Period>,
}

/// An hour of local time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Period {
    date: jiff::civil::Date,
    hour: i8,
}

impl Period {
    fn of(at: SystemTime) -> Option<Self> {
        let zoned = Timestamp::try_from(at)
            .ok()?
            .to_zoned_datetime(TimeZone::system());
        Some(Self {
            date: zoned.date(),
            hour: zoned.hour(),
        })
    }

    /// Whether `now` starts a new period after `self`.
    fn passed(self, now: Self, rotation: RotationPeriod) -> bool {
        match rotation {
            RotationPeriod::Hourly => now != self,
            RotationPeriod::Daily => now.date > self.date,
            RotationPeriod::Never => false,
        }
    }
}

/// Opened, with the bytes cut off its end.
pub(super) struct Opened {
    pub(super) live: LiveFile,
    pub(super) cut: u64,
}

impl LiveFile {
    /// Opens or creates `path` at its end, cutting a partial last line. Syncs
    /// the directory after creating the file when `durable` is set.
    pub(super) fn open(
        path: &Path,
        config: &FileWriterConfig,
        mode: OpenMode,
    ) -> io::Result<Opened> {
        let existed = path.exists();
        let mut options = file_options(config.write.file_mode);
        options.read(true).write(true).create(true);
        if mode == OpenMode::Truncate {
            options.truncate(true);
        }
        let mut file = options.open(path)?;
        let meta = file.metadata()?;
        let len = meta.len();
        let kept = if len == 0 {
            0
        } else {
            let kept = last_newline_end(&mut file, len)?;
            if kept < len {
                file.set_len(kept)?;
                if config.write.durable {
                    file.sync_data()?;
                }
            }
            kept
        };
        file.seek(SeekFrom::Start(kept))?;
        if !existed && config.write.durable {
            sync_dir(parent_of(path))?;
        }
        let last_write = if existed {
            meta.modified().unwrap_or_else(|_| SystemTime::now())
        } else {
            SystemTime::now()
        };
        Ok(Opened {
            live: Self {
                file,
                len: kept,
                period: Period::of(last_write),
            },
            cut: len - kept,
        })
    }

    pub(super) const fn len(&self) -> u64 {
        self.len
    }

    pub(super) fn metadata(&self) -> io::Result<fs::Metadata> {
        self.file.metadata()
    }

    /// The trigger due before writing `incoming` bytes at `now`, if any.
    pub(super) fn rotation_due(
        &self,
        now: SystemTime,
        incoming: usize,
        config: &FileWriterConfig,
    ) -> Option<Trigger> {
        let incoming = u64::try_from(incoming).unwrap_or(u64::MAX);
        if let Some(max) = config.write.max_bytes
            && self.len > 0
            && self.len.saturating_add(incoming) > max
        {
            return Some(Trigger::Size);
        }
        if config.rotation == RotationPeriod::Never {
            return None;
        }
        let (last, now) = (self.period?, Period::of(now)?);
        last.passed(now, config.rotation).then_some(Trigger::Time)
    }

    /// Writes `body`, then syncs it when `durable`. On any error the file
    /// goes back to its length before the call, so a retry never follows half
    /// a line.
    pub(super) fn append(&mut self, body: &[u8], durable: bool, now: SystemTime) -> io::Result<()> {
        let written = self.file.write_all(body).and_then(|()| {
            if durable {
                self.file.sync_data()
            } else {
                Ok(())
            }
        });
        match written {
            Ok(()) => {
                self.len += u64::try_from(body.len()).unwrap_or(u64::MAX);
                self.period = Period::of(now).or(self.period);
                Ok(())
            }
            Err(e) => {
                if let Err(cut) = self.cut_to(self.len) {
                    warn!(error = %e, %cut, "could not cut a partial write from the file");
                }
                Err(e)
            }
        }
    }

    fn cut_to(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.file.seek(SeekFrom::Start(len)).map(drop)
    }

    /// Syncs the file's data to disk.
    pub(super) fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

/// The length of `file` up to and including its last newline: `len` when it
/// ends in one, 0 when it has none.
fn last_newline_end(file: &mut (impl Read + Seek), len: u64) -> io::Result<u64> {
    let mut last = [0_u8; 1];
    file.seek(SeekFrom::Start(len - 1))?;
    file.read_exact(&mut last)?;
    if last[0] == b'\n' {
        return Ok(len);
    }
    let mut buf = Vec::new();
    let mut end = len - 1;
    while end > 0 {
        let start = end.saturating_sub(SCAN_CHUNK);
        buf.resize(usize::try_from(end - start).unwrap_or(0), 0);
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buf)?;
        if let Some(at) = buf.iter().rposition(|&b| b == b'\n') {
            return Ok(start + u64::try_from(at).unwrap_or(0) + 1);
        }
        end = start;
    }
    Ok(0)
}

/// Options that create files with `mode` on Unix, or `0666` before the umask
/// when it is unset. A Windows file takes its folder's ACL.
#[cfg(unix)]
fn file_options(mode: Option<FileMode>) -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    if let Some(mode) = mode {
        options.mode(mode.bits());
    }
    options
}

/// Options that create files with `mode` on Unix. A Windows file takes its
/// folder's ACL.
#[cfg(not(unix))]
fn file_options(_mode: Option<FileMode>) -> OpenOptions {
    OpenOptions::new()
}

/// Creates `dir` and any missing parents, each with `mode` on Unix, or
/// `0777` before the umask when it is unset.
#[cfg_attr(not(unix), allow(unused_variables))]
pub(super) fn create_dir(dir: &Path, mode: Option<FileMode>) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(mode.bits());
    }
    builder.create(dir)
}

/// Syncs a directory so a file created or renamed in it survives a crash.
#[cfg(unix)]
pub(super) fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Windows has no directory handle to sync. NTFS journals the rename itself.
#[cfg(not(unix))]
#[allow(clippy::unnecessary_wraps)]
pub(super) fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

fn parent_of(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// A rotated file beside the live one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rotated {
    path: PathBuf,
    stamp: String,
    number: u32,
    compressed: bool,
}

/// The suffix of `name` if it ends in a codec's extension, such as `.gz`.
fn compressed_suffix(name: &str) -> Option<&'static str> {
    Codec::ALL
        .into_iter()
        .filter_map(Codec::extension)
        .find(|ext| {
            name.strip_suffix(ext)
                .is_some_and(|rest| rest.ends_with('.'))
        })
}

/// Parses `<live>.<stamp>[.N][.<codec extension>]`, refusing every other name
/// so a sidecar such as `<live>.pos` is never touched. Every codec's extension
/// counts, so retention keeps working after `compress_rotated` changes.
fn parse_rotated(live: &OsStr, name: &OsStr) -> Option<(String, u32, bool)> {
    let rest = name
        .to_str()?
        .strip_prefix(live.to_str()?)?
        .strip_prefix('.')?;
    let (rest, compressed) = match compressed_suffix(rest) {
        Some(ext) => (&rest[..rest.len() - ext.len() - 1], true),
        None => (rest, false),
    };
    let (stamp, number) = match rest.split_once('.') {
        Some((stamp, n)) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => {
            (stamp, n.parse().ok()?)
        }
        Some(_) => return None,
        None => (rest, 0),
    };
    let bytes = stamp.as_bytes();
    let well_formed = bytes.len() == STAMP_LEN
        && bytes[8] == b'T'
        && bytes[..8].iter().chain(&bytes[9..]).all(u8::is_ascii_digit);
    well_formed.then(|| (stamp.to_owned(), number, compressed))
}

/// The rotated files of `live`, newest first.
fn rotated_files(live: &Path) -> io::Result<Vec<Rotated>> {
    let Some(live_name) = live.file_name() else {
        return Ok(Vec::new());
    };
    let mut found = Vec::new();
    for entry in fs::read_dir(parent_of(live))? {
        let entry = entry?;
        if let Some((stamp, number, compressed)) = parse_rotated(live_name, &entry.file_name()) {
            found.push(Rotated {
                path: entry.path(),
                stamp,
                number,
                compressed,
            });
        }
    }
    found.sort_by(|a, b| (&b.stamp, b.number).cmp(&(&a.stamp, a.number)));
    Ok(found)
}

/// The [`STAMP_FORMAT`] suffix for `at`, in its zone.
fn stamp(at: &ZonedDateTime) -> String {
    at.strftime(STAMP_FORMAT).to_string()
}

/// Renames the live file to its rotated name and returns that name. The
/// caller has dropped its handle.
pub(super) fn rename_live(live: &Path) -> io::Result<PathBuf> {
    let now = stamp(&ZonedDateTime::now());
    let existing = rotated_files(live)?;
    let mut number = existing
        .first()
        .filter(|newest| newest.stamp == now)
        .map_or(0, |newest| newest.number + 1);
    loop {
        let target = rotated_name(live, &now, number);
        let taken = Codec::ALL
            .into_iter()
            .filter_map(Codec::extension)
            .any(|ext| with_extension(&target, ext).exists());
        if !target.exists() && !taken {
            fs::rename(live, &target)?;
            return Ok(target);
        }
        number += 1;
    }
}

/// `path` with `.<ext>` appended.
fn with_extension(path: &Path, ext: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".");
    name.push(ext);
    PathBuf::from(name)
}

fn rotated_name(live: &Path, stamp: &str, number: u32) -> PathBuf {
    let mut name = live.as_os_str().to_owned();
    name.push(".");
    name.push(stamp);
    if number > 0 {
        name.push(format!(".{number}"));
    }
    PathBuf::from(name)
}

/// Applies the count and age limits to the rotated files of `live`, then
/// compresses those past the newest six. Failures are logged and skipped:
/// retention never fails a write.
pub(super) fn retain(live: &Path, config: &FileWriterConfig, label: &str) {
    let mut kept = match rotated_files(live) {
        Ok(files) => files,
        Err(e) => {
            warn!(label, path = %live.display(), error = %e, "could not list rotated files");
            return;
        }
    };
    let cutoff = Timestamp::now()
        .checked_sub(SignedDuration::from_hours(
            24 * i64::from(config.max_age_days),
        ))
        .ok()
        .map(|at| stamp(&at.to_zoned_datetime(TimeZone::system())))
        .filter(|s| s.len() == STAMP_LEN);
    let max_files = config
        .write
        .max_files
        .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX));
    let mut index = 0;
    kept.retain(|file| {
        let too_old = cutoff.as_ref().is_some_and(|c| file.stamp < *c);
        let expired = too_old || index >= max_files;
        index += 1;
        if !expired {
            return true;
        }
        match fs::remove_file(&file.path) {
            Ok(()) => debug!(label, path = %file.path.display(), "rotated file removed"),
            Err(e) => warn!(label, path = %file.path.display(), error = %e, "could not remove a rotated file"),
        }
        false
    });
    if !config.compress_rotated.is_none() {
        for file in kept
            .iter()
            .skip(KEEP_UNCOMPRESSED)
            .filter(|f| !f.compressed)
        {
            if let Err(e) = compress(&file.path, config) {
                warn!(label, path = %file.path.display(), error = %e, "could not compress a rotated file");
            }
        }
    }
}

/// Compresses `path` to `<path>.<codec extension>` and removes it. A failed
/// copy removes the partial file and leaves `path`.
fn compress(path: &Path, config: &FileWriterConfig) -> io::Result<()> {
    let Some(ext) = config.compress_rotated.codec.extension() else {
        return Ok(());
    };
    let packed = with_extension(path, ext);
    if let Err(e) = write_compressed(path, &packed, config) {
        let _ = fs::remove_file(&packed);
        return Err(e);
    }
    fs::remove_file(path)?;
    debug!(path = %path.display(), "rotated file compressed");
    Ok(())
}

fn write_compressed(source: &Path, packed: &Path, config: &FileWriterConfig) -> io::Result<()> {
    let mut source = File::open(source)?;
    let dest = file_options(config.write.file_mode)
        .write(true)
        .create(true)
        .truncate(true)
        .open(packed)?;
    let mut encoder = config.compress_rotated.encoder(dest)?;
    io::copy(&mut source, &mut encoder)?;
    let dest = encoder.finish()?;
    if config.write.durable {
        dest.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_newline_end_finds_the_end_of_the_last_whole_line() {
        let big_tail = {
            let mut body = b"a\n".to_vec();
            body.extend(std::iter::repeat_n(
                b'x',
                3 * usize::try_from(SCAN_CHUNK).unwrap() + 7,
            ));
            body
        };
        let cases: [(&[u8], u64); 7] = [
            (b"a\n", 2),
            (b"a\nb", 2),
            (b"a\nbb\ncc", 5),
            (b"no newline", 0),
            (b"x", 0),
            (b"\n", 1),
            (&big_tail, 2),
        ];
        for (body, want) in cases {
            let mut cursor = io::Cursor::new(body.to_vec());
            let got = last_newline_end(&mut cursor, body.len() as u64).unwrap();
            assert_eq!(
                got,
                want,
                "{:?}",
                String::from_utf8_lossy(&body[..body.len().min(20)])
            );
        }
    }

    #[test]
    fn parse_rotated_takes_only_rotated_names() {
        let live = OsStr::new("events.ndjson");
        let cases: [(&str, Option<(&str, u32, bool)>); 12] = [
            (
                "events.ndjson.20260302T140000",
                Some(("20260302T140000", 0, false)),
            ),
            (
                "events.ndjson.20260302T140000.3",
                Some(("20260302T140000", 3, false)),
            ),
            (
                "events.ndjson.20260302T140000.gz",
                Some(("20260302T140000", 0, true)),
            ),
            (
                "events.ndjson.20260302T140000.12.gz",
                Some(("20260302T140000", 12, true)),
            ),
            (
                "events.ndjson.20260302T140000.zst",
                Some(("20260302T140000", 0, true)),
            ),
            ("events.ndjson.20260302T140000.xgz", None),
            ("events.ndjson", None),
            ("events.ndjson.pos", None),
            ("events.ndjson.pos.tmp", None),
            ("events.ndjson.2026030T140000", None),
            ("events.ndjson.20260302T140000.x", None),
            ("other.ndjson.20260302T140000", None),
        ];
        for (name, want) in cases {
            let got = parse_rotated(live, OsStr::new(name));
            let want = want.map(|(s, n, c)| (s.to_owned(), n, c));
            assert_eq!(got, want, "{name}");
        }
    }

    #[test]
    fn period_passes_by_rotation() {
        let date = |d| jiff::civil::date(2026, 3, d);
        let at = |d, hour| Period {
            date: date(d),
            hour,
        };
        let cases = [
            (RotationPeriod::Hourly, at(2, 13), at(2, 13), false),
            (RotationPeriod::Hourly, at(2, 13), at(2, 14), true),
            (RotationPeriod::Hourly, at(2, 13), at(3, 13), true),
            (RotationPeriod::Daily, at(2, 13), at(2, 23), false),
            (RotationPeriod::Daily, at(2, 23), at(3, 0), true),
            (RotationPeriod::Daily, at(3, 0), at(2, 23), false),
            (RotationPeriod::Never, at(2, 13), at(9, 1), false),
        ];
        for (rotation, last, now, want) in cases {
            assert_eq!(
                last.passed(now, rotation),
                want,
                "{rotation:?} {last:?} -> {now:?}"
            );
        }
    }

    #[test]
    fn stamp_is_the_civil_time_to_the_second() {
        for (instant, want) in [
            ("2026-03-02T14:00:00+00:00[UTC]", "20260302T140000"),
            (
                "2026-03-02T14:00:00.999999999+00:00[UTC]",
                "20260302T140000",
            ),
            ("2026-12-31T23:59:59+10:00[+10:00]", "20261231T235959"),
            ("2026-01-01T00:00:00-05:00[-05:00]", "20260101T000000"),
            ("2026-07-04T09:05:03+05:30[+05:30]", "20260704T090503"),
        ] {
            let at: ZonedDateTime = instant.parse().unwrap();
            assert_eq!(stamp(&at), want, "{instant}");
        }
    }
}
