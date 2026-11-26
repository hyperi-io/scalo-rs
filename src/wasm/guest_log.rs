// Project:   scalo
// File:      src/wasm/guest_log.rs
// Purpose:   A guest's stdout and stderr as capped, rate-limited log lines
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A guest's stdout and stderr as log lines with a `plugin` field.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::AsyncWrite;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};

/// Longest line logged. The rest of it is cut.
const MAX_LINE_BYTES: usize = 1024;

/// Lines one instance may log a second. The rest are dropped and counted.
const MAX_LINES_PER_SECOND: u32 = 100;

const WINDOW: Duration = Duration::from_secs(1);

/// Which of a guest's streams a line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// What the line splitter hands on.
#[derive(Debug, PartialEq, Eq)]
enum Event {
    Line { text: String, truncated: bool },
    Dropped(u64),
}

/// Splits a stream into lines and applies the length and rate caps.
struct Lines {
    pending: Vec<u8>,
    truncated: bool,
    window: Instant,
    logged: u32,
    dropped: u64,
}

impl Lines {
    fn new(now: Instant) -> Self {
        Self {
            pending: Vec::new(),
            truncated: false,
            window: now,
            logged: 0,
            dropped: 0,
        }
    }

    fn write(&mut self, buf: &[u8], now: Instant, out: &mut impl FnMut(Event)) {
        for chunk in buf.split_inclusive(|b| *b == b'\n') {
            let (text, ends) = match chunk.strip_suffix(b"\n") {
                Some(text) => (text, true),
                None => (chunk, false),
            };
            let room = MAX_LINE_BYTES.saturating_sub(self.pending.len());
            self.truncated |= text.len() > room;
            self.pending
                .extend_from_slice(&text[..text.len().min(room)]);
            if ends {
                self.end_line(now, out);
            }
        }
    }

    fn end_line(&mut self, now: Instant, out: &mut impl FnMut(Event)) {
        if now.duration_since(self.window) >= WINDOW {
            self.report_dropped(out);
            self.window = now;
            self.logged = 0;
        }
        if self.logged < MAX_LINES_PER_SECOND {
            self.logged += 1;
            let text =
                String::from_utf8_lossy(self.pending.strip_suffix(b"\r").unwrap_or(&self.pending));
            out(Event::Line {
                text: text.into_owned(),
                truncated: self.truncated,
            });
        } else {
            self.dropped += 1;
        }
        self.pending.clear();
        self.truncated = false;
    }

    fn report_dropped(&mut self, out: &mut impl FnMut(Event)) {
        if self.dropped > 0 {
            out(Event::Dropped(self.dropped));
            self.dropped = 0;
        }
    }

    /// The last line, if it had no newline, and any count still to report.
    fn finish(&mut self, now: Instant, out: &mut impl FnMut(Event)) {
        if !self.pending.is_empty() {
            self.end_line(now, out);
        }
        self.report_dropped(out);
    }
}

/// One store's guest output, logged with the plugin's name.
#[derive(Clone)]
pub(super) struct GuestLog {
    inner: Arc<Mutex<Sink>>,
}

struct Sink {
    plugin: Arc<str>,
    stream: Stream,
    lines: Lines,
}

impl Sink {
    fn log(plugin: &str, stream: Stream, event: Event) {
        let stream = stream.as_str();
        match event {
            Event::Line { text, truncated } => {
                tracing::info!(plugin, stream, truncated, "wasm guest: {text}");
            }
            Event::Dropped(count) => {
                tracing::warn!(
                    plugin,
                    stream,
                    dropped = count,
                    "wasm guest: lines dropped past {MAX_LINES_PER_SECOND} a second"
                );
            }
        }
    }

    fn write(&mut self, buf: &[u8]) {
        let (plugin, stream) = (Arc::clone(&self.plugin), self.stream);
        self.lines.write(buf, Instant::now(), &mut |event| {
            Self::log(&plugin, stream, event);
        });
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        let (plugin, stream) = (Arc::clone(&self.plugin), self.stream);
        self.lines.finish(Instant::now(), &mut |event| {
            Self::log(&plugin, stream, event);
        });
    }
}

impl GuestLog {
    pub(super) fn new(plugin: Arc<str>, stream: Stream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Sink {
                plugin,
                stream,
                lines: Lines::new(Instant::now()),
            })),
        }
    }
}

impl IsTerminal for GuestLog {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for GuestLog {
    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(Writer(Arc::clone(&self.inner)))
    }
}

struct Writer(Arc<Mutex<Sink>>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .write(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(writes: &[&[u8]], at: &[Duration], finish: bool) -> Vec<Event> {
        let start = Instant::now();
        let mut lines = Lines::new(start);
        let mut events = Vec::new();
        for (buf, offset) in writes.iter().zip(at) {
            lines.write(buf, start + *offset, &mut |e| events.push(e));
        }
        if finish {
            lines.finish(start + at.last().copied().unwrap_or_default(), &mut |e| {
                events.push(e);
            });
        }
        events
    }

    fn line(text: &str) -> Event {
        Event::Line {
            text: text.to_string(),
            truncated: false,
        }
    }

    #[test]
    fn output_becomes_one_event_per_line() {
        let zero = Duration::ZERO;
        let cases: [(&str, &[&[u8]], bool, Vec<Event>); 5] = [
            ("one line", &[b"hello\n"], false, vec![line("hello")]),
            (
                "split across writes",
                &[b"hel", b"lo\nwor", b"ld\n"],
                false,
                vec![line("hello"), line("world")],
            ),
            ("a CRLF", &[b"dos\r\n"], false, vec![line("dos")]),
            ("no newline yet", &[b"partial"], false, vec![]),
            (
                "no newline at the end",
                &[b"partial"],
                true,
                vec![line("partial")],
            ),
        ];
        for (case, writes, finish, want) in cases {
            let at = vec![zero; writes.len()];
            assert_eq!(run(writes, &at, finish), want, "{case}");
        }
    }

    #[test]
    fn a_long_line_is_cut_and_marked() {
        let long = vec![b'x'; MAX_LINE_BYTES + 10];
        let events = run(&[&long, b"\n"], &[Duration::ZERO; 2], false);
        assert_eq!(
            events,
            vec![Event::Line {
                text: "x".repeat(MAX_LINE_BYTES),
                truncated: true,
            }]
        );
    }

    #[test]
    fn lines_past_the_rate_are_dropped_and_counted_in_the_next_window() {
        let burst: Vec<u8> = b"l\n".repeat(MAX_LINES_PER_SECOND as usize + 5);
        let events = run(&[&burst, b"next\n"], &[Duration::ZERO, WINDOW], false);
        let logged = events
            .iter()
            .filter(|e| matches!(e, Event::Line { .. }))
            .count();
        assert_eq!(logged, MAX_LINES_PER_SECOND as usize + 1);
        assert_eq!(events[MAX_LINES_PER_SECOND as usize], Event::Dropped(5));
        assert_eq!(events.last(), Some(&line("next")));
    }
}
