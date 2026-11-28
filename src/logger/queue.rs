// Project:   scalo
// File:      src/logger/queue.rs
// Purpose:   A bounded queue of log lines and the one thread that writes them
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A bounded queue of formatted log lines and the one thread that writes them.
//!
//! An emitting thread hands its line over and carries on. When the queue is
//! full the line is dropped and counted, so a stalled stderr pipe costs lines
//! rather than stalling every thread that logs.

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Lines the queue holds before a new one is dropped.
pub(crate) const CAPACITY: usize = 16_384;

/// How often the writer thread logs the lines dropped since it last did.
const REPORT_EVERY: Duration = Duration::from_secs(10);

/// Lines the writer thread writes before it tells a waiting flush.
const BATCH: u64 = 256;

/// Counts the emitting threads, the writer thread and a flush share.
#[derive(Default)]
struct Counts {
    queued: AtomicU64,
    dropped: AtomicU64,
    written: Mutex<u64>,
    progress: Condvar,
}

/// The sending half of the queue, with its counts.
pub(crate) struct LineQueue {
    sender: SyncSender<Vec<u8>>,
    counts: Arc<Counts>,
}

impl LineQueue {
    /// A queue of `capacity` lines and the thread that writes them to `out`.
    ///
    /// The thread writes what is left and exits once the queue is dropped.
    pub(crate) fn spawn<W: Write + Send + 'static>(out: W, capacity: usize) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let counts = Arc::new(Counts::default());
        let shared = Arc::clone(&counts);
        std::thread::Builder::new()
            .name("log-writer".into())
            .spawn(move || write_lines(&receiver, out, &shared))?;
        Ok(Self { sender, counts })
    }

    /// A writer for one line, queued when it drops.
    pub(crate) fn line(&self) -> QueuedLine<'_> {
        QueuedLine {
            queue: self,
            line: Vec::new(),
        }
    }

    /// Queue `line`, or drop and count it when the queue is full.
    fn push(&self, line: Vec<u8>) {
        if self.sender.try_send(line).is_ok() {
            self.counts.queued.fetch_add(1, Ordering::Release);
            return;
        }
        self.counts.dropped.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        ::metrics::counter!("log_lines_dropped_total").increment(1);
    }

    /// Lines dropped because the queue was full.
    #[cfg(test)]
    fn dropped(&self) -> u64 {
        self.counts.dropped.load(Ordering::Relaxed)
    }

    /// Wait up to `timeout` for every line queued so far to be written, and
    /// say whether it was.
    pub(crate) fn flush(&self, timeout: Duration) -> bool {
        let target = self.counts.queued.load(Ordering::Acquire);
        let deadline = Instant::now() + timeout;
        let mut written = self
            .counts
            .written
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while *written < target {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            written = self
                .counts
                .progress
                .wait_timeout(written, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        true
    }
}

/// One log line, buffered as it is formatted and queued when it drops.
pub(crate) struct QueuedLine<'a> {
    queue: &'a LineQueue,
    line: Vec<u8>,
}

impl Write for QueuedLine<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.line.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for QueuedLine<'_> {
    fn drop(&mut self) {
        if !self.line.is_empty() {
            self.queue.push(std::mem::take(&mut self.line));
        }
    }
}

/// The writer thread: write each line to `out` in order until the queue is
/// dropped, and log the drops every [`REPORT_EVERY`].
fn write_lines<W: Write>(lines: &Receiver<Vec<u8>>, mut out: W, counts: &Counts) {
    let mut reported = 0;
    let mut last_report = Instant::now();
    loop {
        match lines.recv_timeout(REPORT_EVERY) {
            Ok(first) => {
                // A failed write has nowhere to be reported but the stream that failed.
                let _ = out.write_all(&first);
                let mut written = 1;
                while written < BATCH {
                    let Ok(line) = lines.try_recv() else { break };
                    let _ = out.write_all(&line);
                    written += 1;
                }
                let _ = out.flush();
                *counts
                    .written
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) += written;
                counts.progress.notify_all();
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        if last_report.elapsed() >= REPORT_EVERY {
            reported = report_drops(counts, reported);
            last_report = Instant::now();
        }
    }
}

/// Log the lines dropped since `reported`, and return the total.
fn report_drops(counts: &Counts, reported: u64) -> u64 {
    let dropped = counts.dropped.load(Ordering::Relaxed);
    if dropped > reported {
        tracing::warn!(
            dropped = dropped - reported,
            dropped_total = dropped,
            "log lines dropped because the log writer fell behind"
        );
    }
    dropped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::mpsc::Sender;

    use super::super::masking::make_masking_writer;

    /// A writer whose first write blocks until its sender drops.
    struct Stalled(Receiver<()>);

    impl Write for Stalled {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let _ = self.0.recv();
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Bytes written, shared with the test.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Captured {
        fn take(&self) -> String {
            String::from_utf8(std::mem::take(&mut *self.0.lock().unwrap())).unwrap()
        }
    }

    fn leaked<W: Write + Send + 'static>(out: W, capacity: usize) -> &'static LineQueue {
        Box::leak(Box::new(LineQueue::spawn(out, capacity).unwrap()))
    }

    /// A subscriber writing through the masking writer `setup` builds, to
    /// what `inner` makes: text, or JSON with service fields.
    fn subscriber<W, F>(inner: F, is_json: bool) -> Box<dyn tracing::Subscriber + Send + Sync>
    where
        W: Write + 'static,
        F: Fn() -> W + Send + Sync + 'static,
    {
        let fields: HashSet<String> = HashSet::from(["password".to_string()]);
        if is_json {
            let writer =
                make_masking_writer(inner, fields, true, Some("svc".into()), Some("1.0".into()));
            Box::new(
                tracing_subscriber::fmt()
                    .json()
                    .without_time()
                    .with_writer(writer)
                    .finish(),
            )
        } else {
            let writer = make_masking_writer(inner, fields, false, None, None);
            Box::new(
                tracing_subscriber::fmt()
                    .with_ansi(false)
                    .without_time()
                    .with_writer(writer)
                    .finish(),
            )
        }
    }

    /// A tokio task logs and finishes while the writer blocks for good, and
    /// every line past the queue and the one being written is counted.
    #[test]
    fn a_stalled_writer_blocks_no_task_and_drops_are_counted() {
        let (release, stall): (Sender<()>, Receiver<()>) = mpsc::channel();
        let queue = leaked(Stalled(stall), 4);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        for is_json in [false, true] {
            let before = queue.dropped();
            let finished =
                tracing::subscriber::with_default(subscriber(|| queue.line(), is_json), || {
                    runtime.block_on(async {
                        let task = tokio::spawn(async {
                            for i in 0..100 {
                                tracing::info!(i, password = "hunter2", "line");
                            }
                        });
                        tokio::time::timeout(Duration::from_secs(5), task).await
                    })
                });
            assert!(
                matches!(finished, Ok(Ok(()))),
                "the task finished, json {is_json}"
            );
            let dropped = queue.dropped() - before;
            assert!(dropped >= 95, "dropped {dropped}, json {is_json}");
        }
        assert!(
            !queue.flush(Duration::from_millis(50)),
            "a stalled flush gives up"
        );
        drop(release);
    }

    /// Lines come out byte for byte as the masking writer writes them
    /// straight to its destination, and a flush waits for them.
    #[test]
    fn queued_lines_match_a_direct_write_byte_for_byte() {
        let (queued, direct) = (Captured::default(), Captured::default());
        let queue = leaked(queued.clone(), CAPACITY);
        let emit = || {
            tracing::info!(password = "hunter2", user = "bob", "first line");
            tracing::warn!(url = "postgres://svc:secret@db/app", "second line");
        };
        for is_json in [false, true] {
            tracing::subscriber::with_default(subscriber(|| queue.line(), is_json), emit);
            assert!(
                queue.flush(Duration::from_secs(5)),
                "flushed, json {is_json}"
            );
            let to_direct = direct.clone();
            tracing::subscriber::with_default(subscriber(move || to_direct.clone(), is_json), emit);
            let (queued, direct) = (queued.take(), direct.take());
            assert_eq!(queued.lines().count(), 2, "json {is_json}:\n{queued}");
            assert!(
                !queued.contains("hunter2") && !queued.contains("secret@"),
                "{queued}"
            );
            assert_eq!(queued, direct, "json {is_json}");
        }
        assert_eq!(queue.dropped(), 0);
    }

    /// Lines still queued when the queue drops are written, in order, before
    /// the thread exits.
    #[test]
    fn dropping_the_queue_writes_what_is_left() {
        let captured = Captured::default();
        let queue = LineQueue::spawn(captured.clone(), CAPACITY).unwrap();
        let mut expected: Vec<u8> = Vec::new();
        for n in 0..1000 {
            writeln!(expected, "line {n}").unwrap();
            writeln!(queue.line(), "line {n}").unwrap();
        }
        drop(queue);
        let deadline = Instant::now() + Duration::from_secs(5);
        while captured.0.lock().unwrap().len() < expected.len() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(captured.take().as_bytes(), expected.as_slice());
    }
}
