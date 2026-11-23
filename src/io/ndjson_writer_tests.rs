// Project:   scalo
// File:      src/io/ndjson_writer_tests.rs
// Purpose:   NdjsonWriter tests: rotation, retention, sync, modes, cut lines
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::{AsyncNdjsonWriter, NdjsonWriter, REOPEN_BACKOFF_MIN};
use crate::io::{FileWriteOptions, FileWriterConfig, RotationPeriod};

fn test_config(dir: &Path) -> FileWriterConfig {
    FileWriterConfig {
        path: dir.to_path_buf(),
        rotation: RotationPeriod::Daily,
        max_age_days: 1,
        compress_rotated: false,
        write: FileWriteOptions::default(),
    }
}

fn with_write(dir: &Path, rotation: RotationPeriod, write: FileWriteOptions) -> FileWriterConfig {
    FileWriterConfig {
        rotation,
        max_age_days: 30,
        write,
        ..test_config(dir)
    }
}

/// Names in `dir` other than `live`, sorted.
fn rotated_names(dir: &Path, live: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("list")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .filter(|name| name != live)
        .collect();
    names.sort();
    names
}

#[test]
fn test_write_single_line() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = test_config(dir.path());

    let writer = NdjsonWriter::new(&config, "test-svc", "out.ndjson", "test").expect("create");
    assert_eq!(writer.lines_written(), 0);
    assert_eq!(writer.write_errors(), 0);

    writer.write_line(b"{\"msg\":\"hello\"}\n").expect("write");
    assert_eq!(writer.lines_written(), 1);

    let content = std::fs::read_to_string(dir.path().join("test-svc/out.ndjson")).expect("read");
    assert_eq!(content.trim(), r#"{"msg":"hello"}"#);
}

#[test]
fn test_write_buf_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = test_config(dir.path());

    let writer = NdjsonWriter::new(&config, "batch", "out.ndjson", "test").expect("create");

    let mut buf = Vec::new();
    for i in 0..5 {
        buf.extend_from_slice(format!("{{\"n\":{i}}}\n").as_bytes());
    }
    writer.write_buf(&buf, 5).expect("write batch");
    assert_eq!(writer.lines_written(), 5);

    let content = std::fs::read_to_string(dir.path().join("batch/out.ndjson")).expect("read");
    let lines: Vec<&str> = content.trim().lines().collect();
    assert_eq!(lines.len(), 5);
}

/// Rotation by size, by time and by both, and an oversize write into an empty
/// file. A `prior` file is aged two days, so the daily trigger is due.
#[test]
fn rotation_by_size_time_and_both() {
    struct Case {
        name: &'static str,
        rotation: RotationPeriod,
        max_bytes: Option<u64>,
        prior: Option<&'static str>,
        writes: &'static [&'static str],
        live: &'static str,
        rotated: usize,
    }
    let cases = [
        Case {
            name: "size: fits exactly, then rotates",
            rotation: RotationPeriod::Never,
            max_bytes: Some(10),
            prior: None,
            writes: &["aaaa\n", "bbbb\n", "cccc\n"],
            live: "cccc\n",
            rotated: 1,
        },
        Case {
            name: "size: an oversize write lands in an empty file",
            rotation: RotationPeriod::Never,
            max_bytes: Some(3),
            prior: None,
            writes: &["toolong\n"],
            live: "toolong\n",
            rotated: 0,
        },
        Case {
            name: "size: the write after an oversize one rotates",
            rotation: RotationPeriod::Never,
            max_bytes: Some(3),
            prior: None,
            writes: &["toolong\n", "x\n"],
            live: "x\n",
            rotated: 1,
        },
        Case {
            name: "time: an aged file rotates once",
            rotation: RotationPeriod::Daily,
            max_bytes: None,
            prior: Some("old\n"),
            writes: &["a\n", "b\n"],
            live: "a\nb\n",
            rotated: 1,
        },
        Case {
            name: "both: time first, then size",
            rotation: RotationPeriod::Daily,
            max_bytes: Some(5),
            prior: Some("o\n"),
            writes: &["a\n", "bb\n", "c\n"],
            live: "c\n",
            rotated: 2,
        },
        Case {
            name: "neither: never and no size",
            rotation: RotationPeriod::Never,
            max_bytes: None,
            prior: Some("old\n"),
            writes: &["a\n"],
            live: "old\na\n",
            rotated: 0,
        },
        Case {
            name: "hourly: an aged file rotates",
            rotation: RotationPeriod::Hourly,
            max_bytes: None,
            prior: Some("old\n"),
            writes: &["a\n"],
            live: "a\n",
            rotated: 1,
        },
    ];
    for case in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        if let Some(prior) = case.prior {
            let file = dir.path().join("out.ndjson");
            std::fs::write(&file, prior).expect("prior");
            std::fs::File::options()
                .write(true)
                .open(&file)
                .and_then(|f| {
                    f.set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 86_400))
                })
                .expect("age the file");
        }
        let write = FileWriteOptions {
            max_bytes: case.max_bytes,
            ..FileWriteOptions::default()
        };
        let config = with_write(dir.path(), case.rotation, write);
        let writer = NdjsonWriter::new(&config, "", "out.ndjson", "test").expect(case.name);
        for line in case.writes {
            writer.write_line(line.as_bytes()).expect(case.name);
        }
        let live = std::fs::read_to_string(dir.path().join("out.ndjson")).expect("read");
        assert_eq!(live, case.live, "{}", case.name);
        let rotated = rotated_names(dir.path(), "out.ndjson");
        assert_eq!(rotated.len(), case.rotated, "{}: {rotated:?}", case.name);
        let all: String = rotated
            .iter()
            .map(|name| std::fs::read_to_string(dir.path().join(name)).expect("rotated"))
            .collect::<String>();
        let total = all.len() + live.len();
        let written: usize =
            case.writes.iter().map(|w| w.len()).sum::<usize>() + case.prior.map_or(0, str::len);
        assert_eq!(total, written, "{}: no byte lost or repeated", case.name);
    }
}

/// `max_files` keeps that many rotated files, the newest, and unset keeps all.
#[test]
fn max_files_keeps_the_newest() {
    for (max_files, kept) in [(Some(0), 0), (Some(2), 2), (None, 5)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let write = FileWriteOptions {
            max_bytes: Some(1),
            max_files,
            ..FileWriteOptions::default()
        };
        let config = with_write(dir.path(), RotationPeriod::Never, write);
        let writer = NdjsonWriter::new(&config, "", "out.ndjson", "test").expect("create");
        for n in 0..6 {
            writer
                .write_line(format!("{n}\n").as_bytes())
                .expect("write");
        }
        let rotated = rotated_names(dir.path(), "out.ndjson");
        assert_eq!(rotated.len(), kept, "{max_files:?}: {rotated:?}");
        let newest: Vec<String> = rotated
            .iter()
            .map(|name| std::fs::read_to_string(dir.path().join(name)).expect("read"))
            .collect();
        let want: Vec<String> = (5 - kept..5).map(|n| format!("{n}\n")).collect();
        let mut newest = newest;
        newest.sort();
        assert_eq!(newest, want, "{max_files:?} keeps the newest");
    }
}

/// A reader holding the live file open does not stop the rename, and keeps
/// reading the rotated file.
#[test]
fn rotation_renames_a_file_a_reader_holds_open() {
    use std::io::Read as _;

    let dir = tempfile::tempdir().expect("tempdir");
    let write = FileWriteOptions {
        max_bytes: Some(2),
        ..FileWriteOptions::default()
    };
    let config = with_write(dir.path(), RotationPeriod::Never, write);
    let writer = NdjsonWriter::new(&config, "", "out.ndjson", "test").expect("create");
    writer.write_line(b"1\n").expect("write");
    let mut reader = std::fs::File::open(dir.path().join("out.ndjson")).expect("reader");

    writer.write_line(b"2\n").expect("rotates past the reader");
    assert_eq!(rotated_names(dir.path(), "out.ndjson").len(), 1);
    let mut held = String::new();
    reader.read_to_string(&mut held).expect("read");
    assert_eq!(held, "1\n", "the reader follows the renamed file");
    assert_eq!(
        std::fs::read_to_string(dir.path().join("out.ndjson")).expect("read"),
        "2\n"
    );
}

/// Retention only touches names of the rotated form, so a sidecar beside the
/// file survives `max_files: 0`.
#[test]
fn retention_leaves_other_files_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    for name in [
        "out.ndjson.pos",
        "out.ndjson.pos.tmp",
        "out.ndjson.20260302T14",
        "other",
    ] {
        std::fs::write(dir.path().join(name), b"keep").expect("plant");
    }
    let write = FileWriteOptions {
        max_bytes: Some(1),
        max_files: Some(0),
        ..FileWriteOptions::default()
    };
    let config = with_write(dir.path(), RotationPeriod::Never, write);
    let writer = NdjsonWriter::new(&config, "", "out.ndjson", "test").expect("create");
    writer.write_line(b"a\n").expect("write");
    writer.write_line(b"b\n").expect("write");
    assert_eq!(
        rotated_names(dir.path(), "out.ndjson"),
        [
            "other",
            "out.ndjson.20260302T14",
            "out.ndjson.pos",
            "out.ndjson.pos.tmp"
        ]
    );
}

/// With compression on, all but the six newest rotated files are gzipped and
/// read back whole.
#[test]
fn compression_keeps_six_rotated_files_plain() {
    use std::io::Read as _;

    let dir = tempfile::tempdir().expect("tempdir");
    let write = FileWriteOptions {
        max_bytes: Some(1),
        ..FileWriteOptions::default()
    };
    let config = FileWriterConfig {
        compress_rotated: true,
        ..with_write(dir.path(), RotationPeriod::Never, write)
    };
    let writer = NdjsonWriter::new(&config, "", "out.ndjson", "test").expect("create");
    for n in 0..9 {
        writer
            .write_line(format!("{n}\n").as_bytes())
            .expect("write");
    }
    let rotated = rotated_names(dir.path(), "out.ndjson");
    let gz: Vec<&String> = rotated
        .iter()
        .filter(|n| Path::new(n).extension().is_some_and(|e| e == "gz"))
        .collect();
    assert_eq!(rotated.len(), 8, "{rotated:?}");
    assert_eq!(gz.len(), 2, "{rotated:?}");
    let mut text = String::new();
    for name in gz {
        let file = std::fs::File::open(dir.path().join(name)).expect("open");
        flate2::read::GzDecoder::new(file)
            .read_to_string(&mut text)
            .expect("gunzip");
    }
    let mut lines: Vec<&str> = text.lines().collect();
    lines.sort_unstable();
    assert_eq!(lines, ["0", "1"], "the two oldest are compressed");
}

/// Durable or not, the bytes and the rotation are the same. A flush syncs
/// with nothing written yet and after writes.
#[test]
fn durable_on_and_off_write_the_same_bytes() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let write = FileWriteOptions {
            max_bytes: Some(6),
            durable,
            ..FileWriteOptions::default()
        };
        let config = with_write(dir.path(), RotationPeriod::Never, write);
        let writer =
            NdjsonWriter::new(&config, "nested/dir", "out.ndjson", "test").expect("create");
        writer.flush().expect("flush before any write");
        writer.write_buf(b"a\nb\n", 2).expect("write");
        writer.write_buf(b"c\nd\n", 2).expect("write");
        writer.flush().expect("flush");
        let base = dir.path().join("nested/dir");
        assert_eq!(
            std::fs::read_to_string(base.join("out.ndjson")).expect("read"),
            "c\nd\n",
            "durable {durable}"
        );
        assert_eq!(
            rotated_names(&base, "out.ndjson").len(),
            1,
            "durable {durable}"
        );
        assert_eq!(writer.lines_written(), 4);
    }
}

/// The crash-cut fixture: a file left ending mid-line is cut back to its last
/// newline on open, so the next record never joins the half-written one.
#[test]
fn a_partial_last_line_is_cut_before_appending() {
    let cases: [(&str, &[u8], &str, u64); 5] = [
        (
            "mid-line after whole lines",
            b"a\nb\n{\"half\":",
            "a\nb\nc\n",
            8,
        ),
        ("no newline at all", b"{\"half\":", "c\n", 8),
        ("ends in a newline", b"a\n", "a\nc\n", 0),
        ("empty", b"", "c\n", 0),
        ("only carriage return after a line", b"a\n\r", "a\nc\n", 1),
    ];
    for (name, prior, want, cut) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("out.ndjson"), prior).expect("fixture");
        let config = with_write(
            dir.path(),
            RotationPeriod::Never,
            FileWriteOptions::default(),
        );
        let writer = NdjsonWriter::new(&config, "", "out.ndjson", "test").expect(name);
        assert_eq!(writer.cut_bytes(), cut, "{name}: cut bytes");
        writer.write_line(b"c\n").expect(name);
        let got = std::fs::read_to_string(dir.path().join("out.ndjson")).expect("read");
        assert_eq!(got, want, "{name}");
    }
}

/// A file truncated on open is not cut, and keeps nothing it held.
#[test]
fn truncating_drops_what_the_file_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("out.ndjson"), b"old\npartial").expect("fixture");
    let config = with_write(
        dir.path(),
        RotationPeriod::Never,
        FileWriteOptions::default(),
    );
    let writer = NdjsonWriter::truncating(&config, "", "out.ndjson", "test").expect("create");
    assert_eq!(writer.cut_bytes(), 0);
    writer.write_line(b"new\n").expect("write");
    let got = std::fs::read_to_string(dir.path().join("out.ndjson")).expect("read");
    assert_eq!(got, "new\n");
}

/// The process umask, read from a file created asking for 0o777.
#[cfg(unix)]
fn umask(dir: &Path) -> u32 {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let probe = dir.join("umask.probe");
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o777)
        .open(&probe)
        .expect("probe");
    let mode = std::fs::metadata(&probe)
        .expect("stat")
        .permissions()
        .mode()
        & 0o777;
    std::fs::remove_file(&probe).expect("remove probe");
    !mode & 0o777
}

/// Created files and directories take the configured modes, the umask's
/// with none set, and an existing file keeps its own.
#[cfg(unix)]
#[test]
fn created_files_and_directories_take_their_modes() {
    use std::os::unix::fs::PermissionsExt;

    use crate::io::FileMode;

    let mode = |p: &Path| std::fs::metadata(p).expect("stat").permissions().mode() & 0o777;
    let cases = [
        ("unset", FileWriteOptions::default(), 0o666, 0o777),
        (
            "private",
            FileWriteOptions::default().private_by_default(),
            0o600,
            0o700,
        ),
        (
            "configured",
            FileWriteOptions {
                file_mode: FileMode::new(0o640).ok(),
                dir_mode: FileMode::new(0o750).ok(),
                ..FileWriteOptions::default()
            },
            0o640,
            0o750,
        ),
    ];
    for (name, write, file_mode, dir_mode) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let mask = umask(dir.path());
        let config = with_write(
            dir.path(),
            RotationPeriod::Never,
            FileWriteOptions {
                max_bytes: Some(1),
                ..write
            },
        );
        let writer = NdjsonWriter::new(&config, "a/b", "out.ndjson", "test").expect(name);
        writer.write_line(b"1\n").expect(name);
        writer.write_line(b"2\n").expect(name);
        let base = dir.path().join("a/b");
        assert_eq!(
            mode(&dir.path().join("a")),
            dir_mode & !mask,
            "{name}: parent"
        );
        assert_eq!(mode(&base), dir_mode & !mask, "{name}: dir");
        assert_eq!(
            mode(&base.join("out.ndjson")),
            file_mode & !mask,
            "{name}: file"
        );
        for rotated in rotated_names(&base, "out.ndjson") {
            assert_eq!(
                mode(&base.join(&rotated)),
                file_mode & !mask,
                "{name}: {rotated}"
            );
        }
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let existing = dir.path().join("out.ndjson");
    std::fs::write(&existing, b"").expect("create");
    std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    let config = with_write(
        dir.path(),
        RotationPeriod::Never,
        FileWriteOptions::default(),
    );
    let writer = NdjsonWriter::new(&config, "", "out.ndjson", "test").expect("create");
    writer.write_line(b"x\n").expect("write");
    assert_eq!(mode(&existing), 0o644, "an existing file keeps its mode");
}

/// A rotation the directory refuses leaves the write landing in the live
/// file, and rotation runs again once the directory takes it.
#[cfg(unix)]
#[test]
fn a_failed_rotation_writes_on_and_retries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = dir.path().join("svc");
    let write = FileWriteOptions {
        max_bytes: Some(2),
        ..FileWriteOptions::default()
    };
    let config = with_write(dir.path(), RotationPeriod::Never, write);
    let writer = NdjsonWriter::new(&config, "svc", "out.ndjson", "test").expect("create");
    writer.write_line(b"1\n").expect("write");
    if writes_past_permissions(dir.path()) {
        eprintln!("skipping: this process writes files regardless of their mode");
        return;
    }

    let guard = ModeGuard::set(&base, 0o555);
    writer.write_line(b"2\n").expect("lands in the live file");
    writer.write_line(b"3\n").expect("lands in the live file");
    drop(guard);
    assert_eq!(rotated_names(&base, "out.ndjson").len(), 0);

    std::thread::sleep(REOPEN_BACKOFF_MIN * 2);
    writer.write_line(b"4\n").expect("rotates then lands");
    assert_eq!(
        std::fs::read_to_string(base.join("out.ndjson")).expect("read"),
        "4\n"
    );
    assert_eq!(rotated_names(&base, "out.ndjson").len(), 1);
    assert_eq!(writer.write_errors(), 0);
}

#[test]
fn test_write_into_a_directory_replaced_by_a_file_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = test_config(dir.path());
    let writer = NdjsonWriter::new(&config, "gone", "out.ndjson", "dlq").expect("create");

    std::fs::remove_dir_all(dir.path().join("gone")).expect("remove dir");
    std::fs::write(dir.path().join("gone"), b"not a directory").expect("plant file");

    let err = writer.write_line(b"{\"msg\":\"lost\"}\n").unwrap_err();
    // Windows reports a path through a file as not found.
    #[cfg(unix)]
    assert_eq!(err.kind(), std::io::ErrorKind::NotADirectory);
    #[cfg(not(unix))]
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(writer.lines_written(), 0);
    assert_eq!(writer.write_errors(), 1);
}

#[test]
fn test_writer_reopens_once_its_directory_is_restored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = test_config(dir.path());
    let writer = NdjsonWriter::new(&config, "back", "out.ndjson", "dlq").expect("create");
    writer.write_line(b"{\"n\":1}\n").expect("healthy write");

    std::fs::remove_dir_all(dir.path().join("back")).expect("remove dir");
    writer.write_line(b"{\"n\":2}\n").unwrap_err();
    std::fs::create_dir_all(dir.path().join("back")).expect("restore dir");
    std::thread::sleep(REOPEN_BACKOFF_MIN);

    writer
        .write_line(b"{\"n\":3}\n")
        .expect("write after the directory is restored");
    let content = std::fs::read_to_string(dir.path().join("back/out.ndjson")).expect("read");
    assert_eq!(content, "{\"n\":3}\n");
}

/// A deleted file, and one replaced under the path, both refuse writes until
/// the reopen, so no write lands in a file nothing can read at the path.
#[test]
fn test_writer_reopens_after_its_file_is_deleted_or_replaced() {
    for replace in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let writer = NdjsonWriter::new(&config, "del", "out.ndjson", "dlq").expect("create");
        writer.write_line(b"{\"n\":1}\n").expect("healthy write");

        let path = dir.path().join("del/out.ndjson");
        std::fs::remove_file(&path).expect("delete file");
        if replace {
            std::fs::write(&path, b"").expect("replace file");
        }
        writer.write_line(b"{\"n\":2}\n").unwrap_err();
        std::thread::sleep(REOPEN_BACKOFF_MIN);

        writer
            .write_line(b"{\"n\":3}\n")
            .expect("write after the reopen");
        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(content, "{\"n\":3}\n", "replace {replace}");
    }
}

#[test]
fn test_writer_does_not_recreate_a_missing_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = test_config(dir.path());
    let writer = NdjsonWriter::new(&config, "gone", "out.ndjson", "dlq").expect("create");

    std::fs::remove_dir_all(dir.path().join("gone")).expect("remove dir");
    writer.write_line(b"{\"n\":1}\n").unwrap_err();
    std::thread::sleep(REOPEN_BACKOFF_MIN);
    writer.write_line(b"{\"n\":2}\n").unwrap_err();

    assert!(!dir.path().join("gone").exists());
    assert_eq!(writer.write_errors(), 2);
}

/// Restores a directory's mode on drop, so the tempdir is removed after a panic too.
#[cfg(unix)]
struct ModeGuard(std::path::PathBuf);

#[cfg(unix)]
impl ModeGuard {
    fn set(path: &Path, mode: u32) -> Self {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
        Self(path.to_path_buf())
    }
}

#[cfg(unix)]
impl Drop for ModeGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
    }
}

/// A writer whose current file was last written two days ago, so its next
/// write crosses the daily rotation boundary.
fn writer_due_to_rotate(config: &FileWriterConfig, subdir: &str) -> NdjsonWriter {
    let dir = config.path.join(subdir);
    std::fs::create_dir_all(&dir).expect("create dir");
    let file = std::fs::File::create(dir.join("out.ndjson")).expect("create file");
    file.set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 86_400))
        .expect("age the file");
    drop(file);
    NdjsonWriter::new(config, subdir, "out.ndjson", "dlq").expect("create")
}

#[test]
fn test_rotation_into_a_directory_replaced_by_a_file_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let writer = writer_due_to_rotate(&test_config(dir.path()), "swap");

    std::fs::remove_dir_all(dir.path().join("swap")).expect("remove dir");
    std::fs::write(dir.path().join("swap"), b"not a directory").expect("plant file");

    writer.write_line(b"{\"n\":1}\n").unwrap_err();
    writer.write_line(b"{\"n\":2}\n").unwrap_err();
    assert_eq!(writer.write_errors(), 2);
    assert_eq!(writer.lines_written(), 0);
}

#[cfg(unix)]
#[test]
fn test_rotation_into_a_read_only_directory_is_refused_then_recovers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let writer = writer_due_to_rotate(&test_config(dir.path()), "ro");
    let target = dir.path().join("ro");

    std::fs::remove_file(target.join("out.ndjson")).expect("remove file");
    let guard = ModeGuard::set(&target, 0o555);
    writer.write_line(b"{\"n\":1}\n").unwrap_err();

    drop(guard);
    std::thread::sleep(REOPEN_BACKOFF_MIN);
    writer
        .write_line(b"{\"n\":2}\n")
        .expect("write once the directory is writable again");
    let content = std::fs::read_to_string(target.join("out.ndjson")).expect("read");
    assert_eq!(content, "{\"n\":2}\n");
}

#[cfg(unix)]
#[test]
fn test_rotation_after_the_directory_and_its_parent_went_away_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let writer = writer_due_to_rotate(&test_config(dir.path()), "parent/child");
    let parent = dir.path().join("parent");

    std::fs::remove_dir_all(parent.join("child")).expect("remove dir");
    let _guard = ModeGuard::set(&parent, 0o555);

    writer.write_line(b"{\"n\":1}\n").unwrap_err();
    assert!(!parent.join("child").exists());
}

#[test]
fn test_rotation_does_not_recreate_a_removed_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let writer = writer_due_to_rotate(&test_config(dir.path()), "gone");

    std::fs::remove_dir_all(dir.path().join("gone")).expect("remove dir");

    writer.write_line(b"{\"n\":1}\n").unwrap_err();
    assert!(
        !dir.path().join("gone").exists(),
        "a rotation must not recreate a directory that may be an unmounted volume"
    );
}

#[test]
fn test_rotation_with_an_unbounded_max_age_keeps_the_rotated_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = FileWriterConfig {
        max_age_days: u32::MAX,
        ..test_config(dir.path())
    };
    let writer = writer_due_to_rotate(&config, "forever");

    writer.write_line(b"{\"n\":1}\n").expect("rotate and write");
    let content = std::fs::read_to_string(dir.path().join("forever/out.ndjson")).expect("read");
    assert_eq!(content, "{\"n\":1}\n");
    assert_eq!(
        rotated_names(&dir.path().join("forever"), "out.ndjson").len(),
        1
    );
}

/// True when this process can list a directory it has no read permission on.
#[cfg(unix)]
fn reads_past_permissions(dir: &Path) -> bool {
    let probe = dir.join("probe");
    std::fs::create_dir(&probe).expect("create probe");
    let _guard = ModeGuard::set(&probe, 0o300);
    std::fs::read_dir(&probe).is_ok()
}

#[cfg(unix)]
#[test]
fn test_writer_on_an_unreadable_directory_is_refused_at_construction() {
    let dir = tempfile::tempdir().expect("tempdir");
    if reads_past_permissions(dir.path()) {
        eprintln!("skipping: this process reads directories regardless of their mode");
        return;
    }
    let target = dir.path().join("wo");
    std::fs::create_dir(&target).expect("create dir");
    let _guard = ModeGuard::set(&target, 0o300);

    let err = NdjsonWriter::new(&test_config(dir.path()), "wo", "out.ndjson", "dlq")
        .expect_err("an unreadable directory is refused");
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
}

#[cfg(unix)]
#[test]
fn test_writer_does_not_reopen_into_an_unreadable_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    if reads_past_permissions(dir.path()) {
        eprintln!("skipping: this process reads directories regardless of their mode");
        return;
    }
    let config = test_config(dir.path());
    let writer = NdjsonWriter::new(&config, "wo", "out.ndjson", "dlq").expect("create");
    let target = dir.path().join("wo");

    std::fs::remove_file(target.join("out.ndjson")).expect("remove file");
    writer.write_line(b"{\"n\":1}\n").unwrap_err();
    let guard = ModeGuard::set(&target, 0o300);
    std::thread::sleep(REOPEN_BACKOFF_MIN);
    writer.write_line(b"{\"n\":2}\n").unwrap_err();

    drop(guard);
    std::thread::sleep(REOPEN_BACKOFF_MIN * 2);
    writer
        .write_line(b"{\"n\":3}\n")
        .expect("write once the directory is readable again");
}

/// True when this process can open a file it has no write permission on.
#[cfg(unix)]
fn writes_past_permissions(dir: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let probe = dir.join("probe.ro");
    std::fs::write(&probe, b"").expect("create probe");
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o400)).expect("chmod");
    let writable = std::fs::OpenOptions::new()
        .append(true)
        .open(&probe)
        .is_ok();
    std::fs::remove_file(&probe).expect("remove probe");
    writable
}

/// A file the writer cannot open refuses each write, and none is counted
/// as written.
#[cfg(unix)]
#[test]
fn test_write_to_a_file_it_cannot_open_is_refused() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    if writes_past_permissions(dir.path()) {
        eprintln!("skipping: this process writes files regardless of their mode");
        return;
    }
    let target = dir.path().join("ro");
    std::fs::create_dir(&target).expect("create dir");
    std::fs::write(target.join("out.ndjson"), b"").expect("create file");
    std::fs::set_permissions(
        target.join("out.ndjson"),
        std::fs::Permissions::from_mode(0o400),
    )
    .expect("chmod file");
    let writer =
        NdjsonWriter::new(&test_config(dir.path()), "ro", "out.ndjson", "dlq").expect("create");

    writer
        .write_line(b"{\"n\":1}\n")
        .expect_err("a write that reached no file must fail");
    assert_eq!(writer.lines_written(), 0);
    assert_eq!(writer.write_errors(), 1);
    let content = std::fs::read_to_string(target.join("out.ndjson")).expect("read");
    assert!(content.is_empty(), "nothing landed: {content:?}");
}

#[test]
fn test_writer_refuses_a_filename_with_no_final_component() {
    let dir = tempfile::tempdir().expect("tempdir");
    for filename in ["..", ""] {
        let err = NdjsonWriter::new(&test_config(dir.path()), "svc", filename, "dlq")
            .expect_err("names no file");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{filename:?}");
    }
}

#[test]
fn test_debug_label_and_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = test_config(dir.path());

    let writer = NdjsonWriter::new(&config, "svc", "data.ndjson", "output").expect("create");
    let debug = format!("{writer:?}");
    assert!(debug.contains("NdjsonWriter"));
    assert!(debug.contains("output"));
    assert_eq!(writer.label(), "output");
    assert_eq!(writer.output_path(), &dir.path().join("svc"));
    assert_eq!(writer.file_path(), dir.path().join("svc/data.ndjson"));
}

// -----------------------------------------------------------------
// AsyncNdjsonWriter tests -- these prove the async wrapper actually
// moves the sync work off the runtime thread.
// -----------------------------------------------------------------

#[tokio::test]
async fn async_write_line_writes_to_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = test_config(dir.path());
    let writer = NdjsonWriter::new(&cfg, "async-svc", "out.ndjson", "test").expect("create");
    let async_w = AsyncNdjsonWriter::new(writer);

    async_w
        .write_line(b"{\"k\":\"v\"}\n".to_vec())
        .await
        .expect("write_line");
    async_w.flush().await.expect("flush");
    assert_eq!(async_w.lines_written(), 1);
    assert_eq!(async_w.write_errors(), 0);
    assert_eq!(async_w.label(), "test");
    assert_eq!(async_w.output_path(), dir.path().join("async-svc"));

    let body = tokio::fs::read_to_string(dir.path().join("async-svc/out.ndjson"))
        .await
        .expect("read");
    assert_eq!(body.trim(), r#"{"k":"v"}"#);
}

#[tokio::test]
async fn async_writer_from_arc_shares_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = test_config(dir.path());
    let writer = NdjsonWriter::new(&cfg, "share", "out.ndjson", "test").expect("create");
    let shared = Arc::new(writer);
    let a = AsyncNdjsonWriter::from_arc(Arc::clone(&shared));
    let b = AsyncNdjsonWriter::from_arc(Arc::clone(&shared));

    a.write_line(b"{\"a\":1}\n".to_vec()).await.expect("a");
    b.write_buf(b"{\"b\":2}\n".to_vec(), 1).await.expect("b");

    // Both views see the shared counter.
    assert_eq!(a.lines_written(), 2);
    assert_eq!(b.lines_written(), 2);
    assert!(Arc::ptr_eq(&a.shared(), &b.shared()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_writer_does_not_block_runtime() {
    // Prove that concurrent writers + ticker on the same runtime
    // make progress concurrently -- i.e. write_line releases the
    // runtime thread.
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = test_config(dir.path());
    let writer = NdjsonWriter::new(&cfg, "concurrent", "out.ndjson", "test").expect("create");
    let async_w = AsyncNdjsonWriter::new(writer);

    let ticker_fired = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tf = ticker_fired.clone();
    let ticker = tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_millis(2));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        t.tick().await; // burn the t=0 tick
        for _ in 0..20 {
            t.tick().await;
            tf.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });

    let mut writers = Vec::new();
    for _ in 0..4 {
        let w = async_w.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..50_u32 {
                w.write_line(format!("{{\"n\":{i}}}\n").into_bytes())
                    .await
                    .expect("write");
            }
        }));
    }
    for h in writers {
        h.await.expect("writer task");
    }
    ticker.await.expect("ticker task");

    assert_eq!(async_w.lines_written(), 200);
    let ticks = ticker_fired.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        ticks >= 10,
        "ticker fired only {ticks} times -- writers starved the runtime",
    );
}
