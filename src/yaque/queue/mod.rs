// Copied from yaque 0.6.6, src/queue/mod.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! A queue directory: numbered segment files of entries, one sender and one
//! receiver, opened together.

mod receiver;
mod sender;

pub(crate) use receiver::Receiver;
#[cfg(feature = "tiered-sink")]
pub(crate) use receiver::Taken;
pub(crate) use sender::Sender;

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::header::{EOF, Header};
use super::sync::{FileGuard, Shared};
use super::version::{check_queue_version, refuse_earlier_owner};

/// What a queue opens with.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Options {
    /// Bytes a segment holds before the next entry starts a new one.
    pub(crate) segment_bytes: u64,
    /// The receiver's read-ahead buffer.
    pub(crate) read_buffer_bytes: usize,
    /// Most bytes of record buffers the receiver keeps to reuse.
    pub(crate) recycle_bytes: usize,
    /// Checks the last entry of the newest segment at open. One that fails is
    /// cut off with the torn entries.
    pub(crate) whole: Option<fn(&[u8]) -> bool>,
    /// Keeps the newest segment's bytes from a damaged header on, given the
    /// segment and the header's offset, before the open cuts them off.
    pub(crate) damaged_tail: Option<fn(&Path, u64) -> io::Result<()>>,
    /// Reads an entry's records and decoded length from its first
    /// [`SHAPE_BYTES`] bytes. Without it each entry is one record.
    pub(crate) shape: Option<fn(&[u8]) -> Shape>,
    /// The `store` label on the queue's metrics.
    pub(crate) store: &'static str,
}

/// The most leading bytes of an entry a [`Shape`] is read from.
pub(crate) const SHAPE_BYTES: usize = 32;

/// What a store's own prefix says about one entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Shape {
    /// The records it holds.
    pub(crate) records: u64,
    /// The bytes its body decodes to, when the prefix names them.
    pub(crate) decoded: Option<u64>,
}

impl Shape {
    /// One record of unknown decoded length.
    pub(crate) const ONE: Self = Self {
        records: 1,
        decoded: None,
    };

    /// The shape `read` gives `leading`, or one record without a reader.
    fn of(read: Option<fn(&[u8]) -> Self>, leading: &[u8]) -> Self {
        read.map_or(Self::ONE, |read| read(leading))
    }
}

/// The name of a segment file in the queue directory.
pub(crate) fn segment_filename(base: &Path, segment: u64) -> PathBuf {
    base.join(format!("{segment}.q"))
}

/// Opens the queue in `base` for sending and receiving, creating it if absent.
///
/// The two halves share what the sender has written, so the receiver reads
/// only whole entries and wakes when one lands.
///
/// # Errors
///
/// `WouldBlock` when another open holds either half, `InvalidData` for a
/// version stamp or segment name this build does not read, or the I/O error.
pub(crate) fn channel(base: &Path, options: Options) -> io::Result<(Sender, Receiver)> {
    std::fs::create_dir_all(base)?;
    // Before the locks below rewrite the lock files.
    refuse_earlier_owner(base)?;
    let send_lock = lock(base, "send.lock", "sender")?;
    let recv_lock = lock(base, "recv.lock", "receiver")?;
    check_queue_version(base)?;
    let shared = Arc::new(Shared::default());
    let sender = Sender::open(base, options, Arc::clone(&shared), send_lock)?;
    let receiver = Receiver::open(base, options, shared, recv_lock)?;
    Ok((sender, receiver))
}

/// Waits between lock attempts, about a quarter second in all.
const LOCK_RETRY_MS: [u64; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// A process spawned anywhere in this one holds a copy of every open file
/// until it execs, so a lock just released can read as held for that long.
fn lock(base: &Path, name: &str, side: &str) -> io::Result<FileGuard> {
    let path = base.join(name);
    for wait in LOCK_RETRY_MS {
        if let Some(guard) = FileGuard::try_lock(&path)? {
            return Ok(guard);
        }
        std::thread::sleep(std::time::Duration::from_millis(wait));
    }
    FileGuard::try_lock(&path)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "queue {} {side} side is held by another open",
                base.display()
            ),
        )
    })
}

/// The segment numbers in `base`, ascending.
///
/// # Errors
///
/// `InvalidData` for a `.q` file whose name is not a segment number, or the
/// directory read's error.
pub(crate) fn segments(base: &Path) -> io::Result<Vec<u64>> {
    let mut numbers = Vec::new();
    for entry in std::fs::read_dir(base)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("q") {
            continue;
        }
        let number = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} is not a numbered segment", path.display()),
                )
            })?;
        numbers.push(number);
    }
    numbers.sort_unstable();
    Ok(numbers)
}

/// What a walk over one segment's headers found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Scan {
    /// Whole entries after the start offset.
    pub(crate) entries: u64,
    /// The records they hold, by their shape.
    pub(crate) records: u64,
    /// Their payload bytes.
    pub(crate) bytes: u64,
    /// The offset after the last whole entry, or after the end-of-segment marker.
    pub(crate) end: u64,
    /// Whether an end-of-segment marker closes the segment.
    pub(crate) sealed: bool,
    /// Whether the walk stopped at a header that fails its parity check.
    pub(crate) damaged: bool,
    /// The file's length.
    pub(crate) len: u64,
}

/// Payloads longer than this are skipped with a seek, and the next header is
/// read on its own rather than through the buffer.
const SCAN_BUFFER: usize = 64 * 1024;

/// Walks the headers of the segment at `path` from `from`. Payloads are
/// skipped past their first [`SHAPE_BYTES`] when `shape` counts records, and
/// read whole only for the last when `whole` checks it. The walk stops at the
/// end-of-segment marker, at an entry the file does not hold in full, and at
/// a header that fails its parity check, which it reports as `damaged`.
///
/// # Errors
///
/// The open's or a read's error.
pub(crate) fn scan(
    path: &Path,
    from: u64,
    whole: Option<fn(&[u8]) -> bool>,
    shape: Option<fn(&[u8]) -> Shape>,
) -> io::Result<Scan> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut found = Scan {
        end: from,
        len,
        ..Scan::default()
    };
    let mut reader = BufReader::with_capacity(SCAN_BUFFER, file);
    reader.seek(SeekFrom::Start(from))?;
    // After a long seek the buffer is empty, so the next header is read from
    // the file alone instead of refilling the whole buffer for 4 bytes.
    let mut direct = false;
    let mut header = [0_u8; 4];
    while found.end + 4 <= len {
        if direct {
            reader.get_mut().read_exact(&mut header)?;
        } else {
            reader.read_exact(&mut header)?;
        }
        if header == EOF {
            found.end += 4;
            found.sealed = true;
            break;
        }
        let Ok(header) = Header::decode(header) else {
            found.damaged = true;
            break;
        };
        let payload = u64::from(header.len());
        let next = found.end + 4 + payload;
        if next > len {
            break;
        }
        let records = if next == len
            && let Some(whole) = whole
        {
            let mut last = vec![0_u8; usize::try_from(payload).unwrap_or(usize::MAX)];
            if direct {
                reader.get_mut().read_exact(&mut last)?;
            } else {
                reader.read_exact(&mut last)?;
            }
            if !whole(&last) {
                break;
            }
            Shape::of(shape, &last).records
        } else {
            let mut leading = [0_u8; SHAPE_BYTES];
            let peek = if shape.is_some() {
                usize::try_from(payload)
                    .unwrap_or(usize::MAX)
                    .min(SHAPE_BYTES)
            } else {
                0
            };
            if direct {
                reader.get_mut().read_exact(&mut leading[..peek])?;
            } else {
                reader.read_exact(&mut leading[..peek])?;
            }
            let rest = payload - peek as u64;
            if rest > SCAN_BUFFER as u64 {
                reader.seek(SeekFrom::Start(next))?;
                direct = true;
            } else {
                reader.seek_relative(i64::try_from(rest).unwrap_or(i64::MAX))?;
                direct = false;
            }
            Shape::of(shape, &leading[..peek]).records
        };
        found.entries += 1;
        found.records += records;
        found.bytes += payload;
        found.end = next;
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::yaque::test_queue_dir;

    fn options(segment_bytes: u64) -> Options {
        Options {
            segment_bytes,
            read_buffer_bytes: 1024,
            recycle_bytes: 1 << 20,
            whole: None,
            damaged_tail: None,
            shape: None,
            store: "test",
        }
    }

    fn entry(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Header::for_len(payload.len()).unwrap().encode().to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    /// The walk counts whole entries and stops at a torn one, at the
    /// end-of-segment marker and at a damaged header, past the long-payload
    /// seek as well as through the buffer.
    #[test]
    fn a_scan_counts_whole_entries_and_stops_where_the_segment_does() {
        let long = vec![7_u8; SCAN_BUFFER + 10];
        let whole: Vec<u8> = [entry(b"a"), entry(&long), entry(b"bc")].concat();
        let torn = [whole.clone(), entry(b"defg")[..5].to_vec()].concat();
        let sealed = [whole.clone(), EOF.to_vec(), entry(b"x")].concat();
        let damaged = [whole.clone(), b"Asbt".to_vec(), entry(b"y")].concat();
        let end = whole.len() as u64;
        let bytes = 3 + long.len() as u64;
        let rows: [(&str, &[u8], u64, bool, bool); 4] = [
            ("whole", &whole, end, false, false),
            ("torn", &torn, end, false, false),
            ("sealed", &sealed, end + 4, true, false),
            ("damaged header", &damaged, end, false, true),
        ];
        for (name, file, want_end, sealed, damaged_header) in rows {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("0.q");
            std::fs::write(&path, file).unwrap();
            let found = scan(&path, 0, None, None).unwrap();
            assert_eq!(found.entries, 3, "{name}");
            assert_eq!(found.records, 3, "{name}: one record each without a shape");
            let counted = scan(&path, 0, None, Some(first_byte_records)).unwrap();
            assert_eq!(
                counted.records,
                u64::from(b'a') + 7 + u64::from(b'b'),
                "{name}: each entry's shape, the long one read past the buffer"
            );
            assert_eq!(counted.end, want_end, "{name}");
            assert_eq!(found.bytes, bytes, "{name}");
            assert_eq!(found.end, want_end, "{name}");
            assert_eq!(found.sealed, sealed, "{name}");
            assert_eq!(found.damaged, damaged_header, "{name}");
            assert_eq!(found.len, file.len() as u64, "{name}");
            let from = scan(&path, entry(b"a").len() as u64, None, None).unwrap();
            assert_eq!(from.entries, 2, "{name}: walked from the second entry");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.q");
        std::fs::write(&path, &whole).unwrap();
        let refused = scan(&path, 0, Some(|last: &[u8]| last != b"bc"), None).unwrap();
        assert_eq!(refused.entries, 2, "the last entry fails its check");
    }

    /// A test shape: the entry's first byte is its record count.
    pub(super) fn first_byte_records(leading: &[u8]) -> Shape {
        Shape {
            records: leading.first().map_or(1, |n| u64::from(*n)),
            decoded: None,
        }
    }

    /// A read holds to its record cap after the first entry, and takes the
    /// first whatever it holds.
    #[test]
    fn a_read_holds_to_its_record_cap() {
        let (_dir, path) = test_queue_dir();
        let options = Options {
            shape: Some(first_byte_records),
            ..options(1 << 20)
        };
        let (mut sender, mut receiver) = channel(&path, options).unwrap();
        for count in [3_u8, 3, 3] {
            sender.send(&[count], b"entry").unwrap();
        }
        for (cap, want) in [(1, 1), (5, 1), (6, 2), (100, 3)] {
            assert_eq!(
                receiver.read(cap, u64::MAX, None).unwrap().entries,
                want,
                "{cap}"
            );
            receiver.commit(0, None);
        }
        assert_eq!(receiver.queued().unwrap().records, 9);
    }

    #[test]
    fn a_stray_q_file_is_an_error_naming_it() {
        let (_dir, path) = test_queue_dir();
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("foo.q"), b"").unwrap();
        let err = segments(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("foo.q"), "{err}");
        assert!(channel(&path, options(512)).is_err());
    }

    /// Appends raw bytes to a segment, as a writer part way through an entry
    /// leaves them, behind the sender's back.
    fn append_raw(base: &Path, segment: u64, bytes: &[u8]) {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(segment_filename(base, segment))
            .unwrap();
        file.write_all(bytes).unwrap();
    }

    fn send(sender: &mut Sender, payload: &[u8]) {
        sender.send(&[], payload).unwrap();
    }

    /// Reads and commits up to `n` entries.
    fn take(receiver: &mut Receiver, n: usize) -> Vec<Vec<u8>> {
        let taken = receiver.read(n, u64::MAX, None).unwrap();
        let entries = receiver.taken().to_vec();
        receiver.commit(taken.entries, None);
        entries
    }

    /// What the next read gives, rolled back: an entry, `empty`, or the error.
    fn peek(receiver: &mut Receiver) -> String {
        let outcome = match receiver.read(1, u64::MAX, None) {
            Ok(taken) if taken.entries == 0 => "empty".to_string(),
            Ok(_) => format!("{:?}", String::from_utf8_lossy(&receiver.taken()[0])),
            Err(e) => format!("error {:?}", e.kind()),
        };
        receiver.commit(0, None);
        outcome
    }

    fn segment_files(base: &Path) -> usize {
        segments(base).unwrap().len()
    }

    fn disk_on(base: &Path) -> u64 {
        segments(base)
            .unwrap()
            .into_iter()
            .map(|n| std::fs::metadata(segment_filename(base, n)).unwrap().len())
            .sum()
    }

    /// Entries come back in order, whole, across every segment boundary, and
    /// a segment holds entries until it passes its size: at 16 MiB, entries
    /// over 4 MiB share one, where 4 MiB segments gave each its own.
    #[test]
    fn entries_round_trip_in_order_across_segments() {
        let mut rng = crate::yaque::test_rng();
        let small: Vec<Vec<u8>> = (0..300)
            .map(|_| (0..rng() % 128).map(|_| rng().to_le_bytes()[0]).collect())
            .collect();
        let large: Vec<Vec<u8>> = [5 << 20, 5 << 20, 1, 20 << 20]
            .into_iter()
            .map(|len| vec![b'L'; len])
            .collect();
        let rows: [(&str, u64, &[Vec<u8>], usize); 3] = [
            ("small entries, 64-byte segments", 64, &small, 0),
            ("entries over 4 MiB, 16 MiB segments", 16 << 20, &large, 1),
            ("entries over 4 MiB, 4 MiB segments", 4 << 20, &large, 3),
        ];
        for (name, segment_bytes, entries, files) in rows {
            let (_dir, path) = test_queue_dir();
            let (mut sender, mut receiver) = channel(&path, options(segment_bytes)).unwrap();
            for entry in entries {
                send(&mut sender, entry);
            }
            if files > 0 {
                assert_eq!(segment_files(&path), files, "{name}: segments written");
            }
            let mut read = Vec::new();
            while read.len() < entries.len() {
                let batch = take(&mut receiver, 7);
                assert!(!batch.is_empty(), "{name}: stalled after {}", read.len());
                read.extend(batch);
            }
            assert!(read == *entries, "{name}: entries changed or reordered");
            assert_eq!(peek(&mut receiver), "empty", "{name}");
            assert_eq!(sender.disk_bytes(), disk_on(&path), "{name}: disk gauge");
        }
    }

    /// A receiver parked on an empty queue wakes for each send, in the same
    /// segment and across a new one, with no watcher thread.
    #[cfg(feature = "spool")]
    #[test]
    fn a_parked_receiver_wakes_on_each_send() {
        for segment_bytes in [1 << 20, 16] {
            let (_dir, path) = test_queue_dir();
            let (mut sender, mut receiver) = channel(&path, options(segment_bytes)).unwrap();
            let writer = std::thread::spawn(move || {
                for i in 0..20_u8 {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                    sender.send(&[i], b"-after-a-pause").unwrap();
                }
                sender
            });
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let got = runtime.block_on(async {
                let mut got = Vec::new();
                while got.len() < 20 {
                    std::future::poll_fn(|cx| receiver.poll_ready(cx)).await;
                    got.extend(take(&mut receiver, 100).into_iter().map(|e| e[0]));
                }
                got
            });
            drop(writer.join().unwrap());
            assert_eq!(got, (0..20).collect::<Vec<u8>>(), "segment {segment_bytes}");
        }
    }

    /// A transaction commits a prefix and returns the rest. A segment goes
    /// only once every entry in it is committed.
    #[test]
    fn a_commit_keeps_a_prefix_and_returns_the_rest() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(16)).unwrap();
        for payload in [[b'a'; 20], [b'b'; 20], [b'c'; 20]] {
            send(&mut sender, &payload);
        }
        assert_eq!(segment_files(&path), 3);
        assert_eq!(receiver.read(3, u64::MAX, None).unwrap().entries, 3);
        receiver.commit(0, None);
        assert_eq!(segment_files(&path), 3, "a rollback deletes nothing");
        assert_eq!(receiver.read(3, u64::MAX, None).unwrap().entries, 3);
        receiver.commit(2, None);
        assert_eq!(
            segment_files(&path),
            2,
            "the first segment goes, and the second holds its end marker"
        );
        assert_eq!(take(&mut receiver, 3), vec![vec![b'c'; 20]]);
        assert_eq!(sender.disk_bytes(), disk_on(&path));
    }

    /// A read holds to its byte budget after the first entry, and reads the
    /// first entry whatever its size.
    #[test]
    fn a_read_holds_to_its_byte_budget() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        for _ in 0..5 {
            send(&mut sender, &[0; 100]);
        }
        for (budget, want) in [(1, 1), (100, 1), (250, 2), (500, 5)] {
            assert_eq!(
                receiver.read(10, budget, None).unwrap().entries,
                want,
                "{budget}"
            );
            receiver.commit(0, None);
        }
    }

    /// Under a memory guard, a refused reservation stops the read after its
    /// first entry and never before it, and a commit releases what it held.
    #[cfg(feature = "memory")]
    #[test]
    fn a_refused_reservation_stops_the_read_early() {
        use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
        let config = MemoryGuardConfig {
            limit_bytes: 1000,
            ..MemoryGuardConfig::default()
        };
        let guard = MemoryGuard::with_usage_source(config, UsageSource::Reservations);
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        for _ in 0..6 {
            send(&mut sender, &[0; 400]);
        }
        let taken = receiver.read(10, u64::MAX, Some(&guard)).unwrap();
        assert!(taken.held_back, "the guard refused an entry");
        assert!(taken.entries >= 1 && taken.entries < 6, "{taken:?}");
        assert!(receiver.reserved_bytes() > 0);
        receiver.commit(taken.entries, Some(&guard));
        assert_eq!(receiver.reserved_bytes(), 0);
        let mut drained = taken.entries;
        while drained < 6 {
            let taken = receiver.read(10, u64::MAX, Some(&guard)).unwrap();
            assert!(taken.entries >= 1, "the drain went on under pressure");
            drained += taken.entries;
            receiver.commit(taken.entries, Some(&guard));
        }
    }

    /// A segment the receiver has drained is ended and deleted at once, so
    /// the queue stops holding the disk.
    #[test]
    fn a_drained_segment_is_released() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(1600)).unwrap();
        for _ in 0..2 {
            send(&mut sender, &[1; 200]);
        }
        assert_eq!(take(&mut receiver, 10).len(), 2);
        assert!(sender.release_drained(receiver.position()).unwrap());
        receiver.sweep().unwrap();
        assert!(
            !segment_filename(&path, 0).exists(),
            "the drained segment went"
        );
        assert_eq!(sender.disk_bytes(), 0);
        assert_eq!(disk_on(&path), 0);
        send(&mut sender, b"next");
        assert_eq!(take(&mut receiver, 10), vec![b"next".to_vec()]);
        assert!(
            !sender.release_drained(receiver.position()).unwrap(),
            "a segment under a sixteenth of its size is kept"
        );
    }

    /// A rotation that cannot open the next segment loses nothing sent after it.
    #[test]
    fn a_failed_rotation_loses_no_later_entry() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(16)).unwrap();
        send(&mut sender, b"first-record-twenty!");
        std::fs::create_dir(segment_filename(&path, 1)).unwrap();
        assert!(sender.send(&[], b"refused").is_err());
        std::fs::remove_dir(segment_filename(&path, 1)).unwrap();
        send(&mut sender, b"accepted-after");
        assert_eq!(
            take(&mut receiver, 1),
            vec![b"first-record-twenty!".to_vec()]
        );
        assert_eq!(peek(&mut receiver), "\"accepted-after\"");
    }

    #[test]
    fn an_entry_past_the_header_limit_is_an_error() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, _receiver) = channel(&path, options(1 << 20)).unwrap();
        let big = vec![0_u8; 1 << 26];
        let err = sender.send(&[], &big).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            sender.send(&[0; 2], &big[..(1 << 26) - 2]).is_err(),
            "the prefix counts"
        );
    }

    /// Bytes written behind the sender's back, as a torn or half-written
    /// entry, are never read. A rollback redelivers what it held, a commit
    /// saves a whole-entry position, and a reopen keeps the entry once whole
    /// or cuts it off while torn.
    #[test]
    fn a_half_written_entry_is_never_read() {
        let torn_header = Header::for_len(100).unwrap().encode();
        // Rolled back beside a half-written entry, the batch comes back whole.
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        send(&mut sender, b"zero-record-bytes");
        assert_eq!(take(&mut receiver, 1).len(), 1);
        send(&mut sender, b"first-record");
        append_raw(&path, 0, &torn_header);
        append_raw(&path, 0, &[7; 10]);
        assert_eq!(receiver.read(10, u64::MAX, None).unwrap().entries, 1);
        receiver.commit(0, None);
        append_raw(&path, 0, &[7; 90]);
        assert_eq!(peek(&mut receiver), "\"first-record\"");
        drop((sender, receiver));

        // Committed beside it, the saved position is at the whole entry, and
        // the reopen reads it once the rest landed.
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        send(&mut sender, b"first-record");
        append_raw(&path, 0, &torn_header);
        append_raw(&path, 0, &[7; 10]);
        assert_eq!(take(&mut receiver, 10).len(), 1);
        drop((sender, receiver));
        append_raw(&path, 0, &[7; 90]);
        let (_sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        assert_eq!(take(&mut receiver, 10), vec![vec![7; 100]]);

        // Still torn at the reopen, it is cut off and later entries read.
        let (_dir, path) = test_queue_dir();
        let (mut sender, receiver) = channel(&path, options(1 << 20)).unwrap();
        send(&mut sender, b"first-record");
        append_raw(&path, 0, &torn_header);
        append_raw(&path, 0, &[7; 10]);
        drop((sender, receiver));
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        send(&mut sender, b"after-restart");
        assert_eq!(
            take(&mut receiver, 10),
            vec![b"first-record".to_vec(), b"after-restart".to_vec()]
        );
        assert_eq!(sender.disk_bytes(), disk_on(&path));
    }

    /// A segment closed by its marker before the next existed, as a kill
    /// between the two leaves it, takes no entry after a restart.
    #[test]
    fn a_sealed_segment_takes_no_entry_after_a_restart() {
        let (_dir, path) = test_queue_dir();
        drop(channel(&path, options(1 << 20)).unwrap());
        std::fs::write(
            segment_filename(&path, 0),
            [entry(b"first-record"), EOF.to_vec()].concat(),
        )
        .unwrap();
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        send(&mut sender, b"after-restart");
        assert_eq!(
            take(&mut receiver, 10),
            vec![b"first-record".to_vec(), b"after-restart".to_vec()]
        );
    }

    /// A commit whose segment cannot be deleted still commits, and the
    /// receiver and a reopen go on.
    #[test]
    fn a_segment_that_cannot_be_deleted_does_not_wedge_the_receiver() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(16)).unwrap();
        for byte in *b"abc" {
            send(&mut sender, &[byte; 20]);
        }
        assert_eq!(receiver.read(3, u64::MAX, None).unwrap().entries, 3);
        // Segment 1 cannot be removed, as when another program holds it open on Windows.
        std::fs::remove_file(segment_filename(&path, 1)).unwrap();
        std::fs::create_dir(segment_filename(&path, 1)).unwrap();
        std::fs::write(segment_filename(&path, 1).join("held"), b"x").unwrap();
        receiver.commit(3, None);
        std::fs::remove_dir_all(segment_filename(&path, 1)).unwrap();
        send(&mut sender, &[b'd'; 20]);
        assert_eq!(peek(&mut receiver), format!("{:?}", "d".repeat(20)));
        drop((sender, receiver));
        let (_sender, mut receiver) = channel(&path, options(16)).unwrap();
        assert_eq!(take(&mut receiver, 10), vec![vec![b'd'; 20]]);
    }

    /// A read error returns at once: never a spin, and never an entry lost.
    #[test]
    fn a_read_error_returns_and_the_next_read_meets_it() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(16)).unwrap();
        send(&mut sender, b"first-record-twenty!");
        send(&mut sender, b"second-record");
        // The next segment reads back an error, as a failing disk does.
        std::fs::remove_file(segment_filename(&path, 1)).unwrap();
        std::fs::create_dir(segment_filename(&path, 1)).unwrap();
        let taken = receiver.read(10, u64::MAX, None).unwrap();
        assert_eq!(taken.entries, 1, "the read stops at the error");
        receiver.commit(1, None);
        assert!(
            receiver.read(10, u64::MAX, None).is_err(),
            "the next read meets it"
        );
    }

    /// The committed position is saved every 250 entries however they are
    /// batched, and again on drop.
    #[test]
    fn the_position_is_saved_every_250_entries() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        for _ in 0..1000 {
            send(&mut sender, b"r");
        }
        let mut taken = 0;
        while taken < 994 {
            taken += take(&mut receiver, 7).len();
        }
        let saved = std::fs::read(path.join(crate::yaque::RECV_METADATA)).unwrap();
        let position = u64::from_be_bytes(saved[8..].try_into().unwrap());
        assert!(
            position >= 5 * 744,
            "saved at {position} after {taken} entries"
        );
    }

    /// A write that fails part way, here past RLIMIT_FSIZE, is cut back, so
    /// the entries sent after it are read.
    #[cfg(all(unix, feature = "tiered-sink"))]
    #[test]
    #[ignore = "caps RLIMIT_FSIZE for the whole process -- run: cargo nextest run --run-ignored all --ignore-default-filter"]
    #[allow(unsafe_code)]
    fn a_failed_large_append_keeps_later_entries_readable() {
        let (_dir, path) = test_queue_dir();
        let (mut sender, mut receiver) = channel(&path, options(1 << 20)).unwrap();
        send(&mut sender, &[1; 100]);
        let mut old = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: libc calls on stack values this function owns.
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            libc::getrlimit(libc::RLIMIT_FSIZE, &raw mut old);
            let capped = libc::rlimit {
                rlim_cur: 5000,
                rlim_max: old.rlim_max,
            };
            libc::setrlimit(libc::RLIMIT_FSIZE, &raw const capped);
        }
        let failed = sender.send(&[], &[2; 16 * 1024]);
        // SAFETY: as above.
        unsafe {
            libc::setrlimit(libc::RLIMIT_FSIZE, &raw const old);
        }
        let err = failed.expect_err("the capped write fails");
        assert_eq!(err.kind(), io::ErrorKind::FileTooLarge, "{err}");
        assert_eq!(disk_on(&path), 104, "the torn write was cut back");
        send(&mut sender, b"accepted-after");
        assert_eq!(
            take(&mut receiver, 10),
            vec![vec![1; 100], b"accepted-after".to_vec()]
        );
    }

    /// A second open of a queue another open holds is refused as `WouldBlock`.
    #[test]
    fn a_second_open_is_refused() {
        let (_dir, path) = test_queue_dir();
        let first = channel(&path, options(512)).unwrap();
        let err = channel(&path, options(512)).err().expect("held");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "{err}");
        drop(first);
        channel(&path, options(512)).expect("released");
    }

    /// Every file under `dir` and its bytes.
    fn snapshot(dir: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut files = std::collections::BTreeMap::new();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(snapshot(&path));
            } else {
                files.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
        files
    }

    /// A drained 0.6 queue whose lock file names a 0.6 owner is refused on
    /// every open, and no refused open changes a file in it, so a restart
    /// loop never clears it under the running 0.6 process.
    #[test]
    fn an_earlier_owner_is_refused_without_touching_the_queue() {
        let (_dir, path) = test_queue_dir();
        std::fs::create_dir_all(path.join("version")).unwrap();
        std::fs::write(path.join("version").join("contents"), "0.6.6\n").unwrap();
        let segment = [entry(b"first"), entry(b"second")].concat();
        std::fs::write(segment_filename(&path, 0), &segment).unwrap();
        let mut saved = [0_u8; 16];
        saved[8..].copy_from_slice(&(segment.len() as u64).to_be_bytes());
        std::fs::write(path.join(crate::yaque::RECV_METADATA), saved).unwrap();
        std::fs::write(path.join("send.lock"), "pid=4242\ntoken=7\n").unwrap();
        let before = snapshot(&path);
        for open in 1..=2 {
            let err = channel(&path, options(512)).err().expect("refused");
            assert_eq!(err.kind(), io::ErrorKind::Unsupported, "open {open}: {err}");
            assert!(err.to_string().contains("pid 4242"), "open {open}: {err}");
            assert_eq!(snapshot(&path), before, "open {open} changed the queue");
        }
    }

    /// A copy of a lock's handle, as a spawned child holds until it execs,
    /// delays the open while it lasts and refuses it past the retries.
    #[test]
    fn an_open_waits_out_a_briefly_held_copy_of_the_lock() {
        for (held_ms, opens) in [(30, true), (2_000, false)] {
            let (_dir, path) = test_queue_dir();
            drop(channel(&path, options(512)).unwrap());
            let copy = std::fs::OpenOptions::new()
                .write(true)
                .open(path.join("send.lock"))
                .unwrap();
            copy.try_lock().unwrap();
            let release = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(held_ms));
                drop(copy);
            });
            let opened = channel(&path, options(512));
            assert_eq!(opened.is_ok(), opens, "held {held_ms} ms");
            if let Err(e) = opened {
                assert_eq!(e.kind(), io::ErrorKind::WouldBlock, "{e}");
            }
            release.join().unwrap();
        }
    }
}
