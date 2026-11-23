// Project:   scalo
// File:      src/transport/syslog/relp.rs
// Purpose:   RELP client session: open, syslog with a window of acks, close
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Reliable Event Logging Protocol, client side.
//!
//! A frame is `TXNR SP COMMAND SP DATALEN [SP DATA] LF`. The client opens the
//! session, sends each message as a `syslog` command, and the receiver answers
//! each transaction number with `rsp`, whose data starts `200` for taken or
//! `500` for refused. Up to `window` messages are sent before an answer is
//! awaited. A receiver closing the session sends `serverclose` on txnr 0.

use std::collections::VecDeque;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::stream::Stream;

/// The offers this client opens a session with.
const OPEN_OFFERS: &[u8] = b"relp_version=0\nrelp_software=scalo\ncommands=syslog";

/// The largest frame accepted from a receiver. Its answers are short.
const MAX_INBOUND_FRAME: usize = 64 * 1024;

/// Transaction numbers run 1 to this, then wrap to 1.
const MAX_TXNR: u32 = 999_999_999;

/// How long `close` waits for the receiver to answer.
const CLOSE_WAIT: Duration = Duration::from_secs(1);

/// One frame read from the receiver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelpFrame {
    pub(crate) txnr: u32,
    pub(crate) command: String,
    pub(crate) data: Vec<u8>,
}

/// Why a RELP exchange stopped.
#[derive(Debug)]
pub(crate) enum RelpError {
    /// The connection failed or closed: unanswered messages are sent again.
    Io(io::Error),
    /// The receiver sent something that is not RELP, or answered a txnr not
    /// in flight.
    Protocol(String),
    /// The receiver refused the session at open.
    Refused(String),
}

impl std::fmt::Display for RelpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Protocol(what) => write!(f, "not RELP: {what}"),
            Self::Refused(why) => write!(f, "session refused: {why}"),
        }
    }
}

impl From<io::Error> for RelpError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// The answer to one message, by its index in the batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Answer {
    /// `200`: the receiver took it.
    Taken,
    /// Any other code: the receiver refused it, with its text.
    Refused(String),
}

/// An open RELP session.
pub(super) struct RelpSession {
    stream: Stream,
    next_txnr: u32,
    inbound: Vec<u8>,
    outbound: Vec<u8>,
}

impl RelpSession {
    /// Open a session on `stream` and check the receiver takes `syslog`.
    pub(super) async fn open(stream: Stream) -> Result<Self, RelpError> {
        let mut session = Self {
            stream,
            next_txnr: 1,
            inbound: Vec::new(),
            outbound: Vec::new(),
        };
        let txnr = session.take_txnr();
        session.outbound.clear();
        encode_frame(txnr, "open", OPEN_OFFERS, &mut session.outbound);
        session.stream.write_all(&session.outbound).await?;
        session.stream.flush().await?;
        let frame = session.read_frame().await?;
        if frame.command != "rsp" || frame.txnr != txnr {
            return Err(RelpError::Protocol(format!(
                "open answered by `{} {}`",
                frame.txnr, frame.command
            )));
        }
        let (code, text, offers) = split_rsp(&frame.data);
        if code != 200 {
            return Err(RelpError::Refused(format!("{code} {text}")));
        }
        let takes_syslog = offers.split(|&b| b == b'\n').any(|offer| {
            offer
                .strip_prefix(b"commands=")
                .is_some_and(|list| list.split(|&b| b == b',').any(|c| c == b"syslog"))
        });
        if !takes_syslog {
            return Err(RelpError::Refused(
                "the receiver does not offer the syslog command".to_string(),
            ));
        }
        Ok(session)
    }

    /// Send `messages`, the ones whose `answers` entry is `None`, keeping at
    /// most `window` unanswered, and fill in each answer as it arrives.
    ///
    /// After a refusal no further message is sent, and the ones in flight are
    /// still awaited. On an error the entries still `None` were not answered
    /// and are sent again on a new session.
    pub(super) async fn send(
        &mut self,
        messages: &[&[u8]],
        answers: &mut [Option<Answer>],
        window: usize,
    ) -> Result<(), RelpError> {
        let unanswered: Vec<usize> = (0..messages.len())
            .filter(|&i| answers[i].is_none())
            .collect();
        let mut todo = unanswered.into_iter();
        let mut in_flight: VecDeque<(u32, usize)> = VecDeque::with_capacity(window);
        let mut refused = false;
        loop {
            self.outbound.clear();
            while !refused && in_flight.len() < window {
                let Some(index) = todo.next() else { break };
                let txnr = self.take_txnr();
                encode_frame(txnr, "syslog", messages[index], &mut self.outbound);
                in_flight.push_back((txnr, index));
            }
            if !self.outbound.is_empty() {
                self.stream.write_all(&self.outbound).await?;
                self.stream.flush().await?;
            }
            if in_flight.is_empty() {
                return Ok(());
            }
            let frame = self.read_frame().await?;
            match frame.command.as_str() {
                "rsp" => {
                    let Some((_, index)) = in_flight
                        .iter()
                        .position(|&(t, _)| t == frame.txnr)
                        .and_then(|at| in_flight.remove(at))
                    else {
                        return Err(RelpError::Protocol(format!(
                            "an answer to txnr {}, which is not in flight",
                            frame.txnr
                        )));
                    };
                    let (code, text, _) = split_rsp(&frame.data);
                    answers[index] = Some(if code == 200 {
                        Answer::Taken
                    } else {
                        refused = true;
                        Answer::Refused(format!("{code} {text}"))
                    });
                }
                "serverclose" => {
                    return Err(RelpError::Io(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "the receiver closed the session",
                    )));
                }
                other => {
                    return Err(RelpError::Protocol(format!("unexpected command `{other}`")));
                }
            }
        }
    }

    /// Close the session, waiting a moment for the receiver's answer.
    pub(super) async fn close(mut self) {
        let txnr = self.take_txnr();
        self.outbound.clear();
        encode_frame(txnr, "close", b"", &mut self.outbound);
        let closing = async {
            self.stream.write_all(&self.outbound).await?;
            self.stream.flush().await?;
            let _ = self.read_frame().await?;
            self.stream.shutdown().await?;
            Ok::<(), RelpError>(())
        };
        if let Ok(Err(e)) = tokio::time::timeout(CLOSE_WAIT, closing).await {
            tracing::debug!(error = %e, "syslog: RELP close not answered cleanly");
        }
    }

    /// The stream, to check for a close between batches.
    pub(super) fn stream_mut(&mut self) -> &mut Stream {
        &mut self.stream
    }

    fn take_txnr(&mut self) -> u32 {
        let txnr = self.next_txnr;
        self.next_txnr = if txnr >= MAX_TXNR { 1 } else { txnr + 1 };
        txnr
    }

    /// The next whole frame from the receiver.
    async fn read_frame(&mut self) -> Result<RelpFrame, RelpError> {
        loop {
            if let Some((frame, len)) = parse_frame(&self.inbound)? {
                self.inbound.drain(..len);
                return Ok(frame);
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await?;
            if read == 0 {
                return Err(RelpError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the receiver closed the connection",
                )));
            }
            self.inbound.extend_from_slice(&chunk[..read]);
        }
    }
}

/// Append one frame to `out`.
pub(crate) fn encode_frame(txnr: u32, command: &str, data: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(txnr.to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(command.as_bytes());
    out.push(b' ');
    out.extend_from_slice(data.len().to_string().as_bytes());
    if !data.is_empty() {
        out.push(b' ');
        out.extend_from_slice(data);
    }
    out.push(b'\n');
}

/// The frame at the front of `buf` and its length, or `None` when it is not
/// all there yet.
///
/// # Errors
///
/// [`RelpError::Protocol`] for bytes that cannot start a frame, or a frame
/// over the inbound limit.
pub(crate) fn parse_frame(buf: &[u8]) -> Result<Option<(RelpFrame, usize)>, RelpError> {
    let bad = |what: &str| Err(RelpError::Protocol(what.to_string()));
    let mut at = 0;
    let Some(txnr) = field(buf, &mut at, 9, u8::is_ascii_digit) else {
        return incomplete_or(buf, at, 9, "txnr is not 1 to 9 digits");
    };
    let Some(command) = field(buf, &mut at, 32, u8::is_ascii_alphabetic) else {
        return incomplete_or(buf, at, 32, "command is not 1 to 32 letters");
    };
    let digits = buf[at..].iter().take_while(|b| b.is_ascii_digit()).count();
    if digits > 9 {
        return bad("datalen is over 9 digits");
    }
    let Some(&after) = buf.get(at + digits) else {
        return Ok(None);
    };
    if digits == 0 {
        return bad("datalen is missing");
    }
    let datalen: usize = parse_digits(&buf[at..at + digits]);
    if datalen > MAX_INBOUND_FRAME {
        return bad("frame is over the inbound limit");
    }
    at += digits;
    let data = match (datalen, after) {
        (0, b'\n') => {
            at += 1;
            Vec::new()
        }
        (0, _) => return bad("a frame with no data does not end in LF"),
        (_, b' ') => {
            let start = at + 1;
            let end = start + datalen;
            let Some(&trailer) = buf.get(end) else {
                return Ok(None);
            };
            if trailer != b'\n' {
                return bad("data is not followed by LF");
            }
            at = end + 1;
            buf[start..end].to_vec()
        }
        _ => return bad("datalen is not followed by a space"),
    };
    let frame = RelpFrame {
        txnr: u32::try_from(parse_digits(txnr)).unwrap_or(u32::MAX),
        command: String::from_utf8_lossy(command).into_owned(),
        data,
    };
    Ok(Some((frame, at)))
}

/// A field of 1 to `max` bytes matching `valid` then one space, moving `at`
/// past it. `None` when the field is not complete or not well formed.
fn field<'a>(
    buf: &'a [u8],
    at: &mut usize,
    max: usize,
    valid: fn(&u8) -> bool,
) -> Option<&'a [u8]> {
    let rest = &buf[*at..];
    let len = rest.iter().take_while(|b| valid(b)).count();
    if len == 0 || len > max || rest.get(len) != Some(&b' ') {
        return None;
    }
    *at += len + 1;
    Some(&rest[..len])
}

/// `Ok(None)` when the field at `at` could still complete, else the error.
fn incomplete_or(
    buf: &[u8],
    at: usize,
    max: usize,
    what: &str,
) -> Result<Option<(RelpFrame, usize)>, RelpError> {
    let rest = &buf[at..];
    let len = rest
        .iter()
        .take_while(|b| b.is_ascii_alphanumeric())
        .count();
    if len == rest.len() && len <= max {
        Ok(None)
    } else {
        Err(RelpError::Protocol(what.to_string()))
    }
}

fn parse_digits(digits: &[u8]) -> usize {
    digits
        .iter()
        .fold(0usize, |n, d| n * 10 + usize::from(d - b'0'))
}

/// An `rsp` frame's code, its text, and the offers after its first LF.
pub(crate) fn split_rsp(data: &[u8]) -> (u16, String, &[u8]) {
    let (head, offers) = match data.iter().position(|&b| b == b'\n') {
        Some(lf) => (&data[..lf], &data[lf + 1..]),
        None => (data, &data[data.len()..]),
    };
    let (code, text) = match head.iter().position(|&b| b == b' ') {
        Some(sp) => (&head[..sp], &head[sp + 1..]),
        None => (head, &head[head.len()..]),
    };
    let code = std::str::from_utf8(code)
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    (code, String::from_utf8_lossy(text).into_owned(), offers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_encode_with_and_without_data() {
        let mut out = Vec::new();
        encode_frame(1, "open", OPEN_OFFERS, &mut out);
        encode_frame(2, "syslog", b"<14>1 - - - - - - hi", &mut out);
        encode_frame(3, "close", b"", &mut out);
        assert_eq!(
            out,
            b"1 open 50 relp_version=0\nrelp_software=scalo\ncommands=syslog\n\
              2 syslog 20 <14>1 - - - - - - hi\n3 close 0\n"
        );
    }

    #[test]
    fn frames_parse_whole_wait_when_cut_and_refuse_garbage() {
        // (case, input, frame or None to wait, else the error starts with)
        type Want = Result<Option<(u32, &'static str, &'static [u8], usize)>, &'static str>;
        let cases: Vec<(&str, &[u8], Want)> = vec![
            (
                "rsp",
                b"7 rsp 6 200 OK\n",
                Ok(Some((7, "rsp", b"200 OK", 15))),
            ),
            (
                "no data",
                b"0 serverclose 0\n",
                Ok(Some((0, "serverclose", b"", 16))),
            ),
            (
                "two frames take the first",
                b"1 rsp 6 200 OK\n2 rsp 6 200 OK\n",
                Ok(Some((1, "rsp", b"200 OK", 15))),
            ),
            (
                "data holds LF",
                b"1 rsp 8 200 OK\nx\n",
                Ok(Some((1, "rsp", b"200 OK\nx", 17))),
            ),
            ("empty", b"", Ok(None)),
            ("cut in txnr", b"12", Ok(None)),
            ("cut in command", b"12 rs", Ok(None)),
            ("cut in datalen", b"12 rsp 6", Ok(None)),
            ("cut in data", b"12 rsp 6 200", Ok(None)),
            ("cut before trailer", b"12 rsp 6 200 OK", Ok(None)),
            ("letters for txnr", b"x rsp 0\n", Err("not RELP: txnr")),
            ("digits for command", b"1 2 0\n", Err("not RELP: command")),
            (
                "no trailer",
                b"1 rsp 2 OKX",
                Err("not RELP: data is not followed"),
            ),
            (
                "zero then data",
                b"1 rsp 0 x\n",
                Err("not RELP: a frame with no data"),
            ),
            ("huge", b"1 rsp 999999 ", Err("not RELP: frame is over")),
        ];
        for (case, input, want) in cases {
            let got = parse_frame(input);
            match (got, want) {
                (Ok(None), Ok(None)) => {}
                (Ok(Some((frame, len))), Ok(Some((txnr, command, data, want_len)))) => {
                    assert_eq!(
                        (
                            frame.txnr,
                            frame.command.as_str(),
                            frame.data.as_slice(),
                            len
                        ),
                        (txnr, command, data, want_len),
                        "{case}"
                    );
                }
                (Err(e), Err(prefix)) => {
                    assert!(e.to_string().starts_with(prefix), "{case}: {e}");
                }
                (got, want) => panic!("{case}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[test]
    fn rsp_data_splits_into_code_text_and_offers() {
        let (code, text, offers) = split_rsp(b"200 OK\nrelp_version=0\ncommands=syslog");
        assert_eq!((code, text.as_str()), (200, "OK"));
        assert_eq!(offers, b"relp_version=0\ncommands=syslog");
        let (code, text, offers) = split_rsp(b"500 queue full");
        assert_eq!((code, text.as_str(), offers), (500, "queue full", &b""[..]));
        assert_eq!(split_rsp(b"").0, 0);
    }
}
