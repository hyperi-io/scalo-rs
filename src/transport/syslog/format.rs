// Project:   scalo
// File:      src/transport/syslog/format.rs
// Purpose:   RFC 5424 and RFC 3164 message headers
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io::Write as _;

use jiff::Timestamp;
use jiff::tz::TimeZone;

use super::config::{SyslogConfig, SyslogFormat};
use crate::transport::work_batch::{Record, TIME_UNIX_NANO_HEADER, parse_time_unix_nano};

/// The longest RFC 5424 HOSTNAME.
const HOSTNAME_MAX: usize = 255;
/// The longest RFC 5424 MSGID.
const MSG_ID_MAX: usize = 32;

/// Writes one record as one syslog message.
#[derive(Debug, Clone)]
pub(super) struct Formatter {
    format: SyslogFormat,
    pri: u8,
    passthrough: bool,
    /// Send an LF inside a message as `#012`, for LF framing.
    escape_lf: bool,
    hostname: String,
    /// The record header HOSTNAME is read from, in place of `hostname`.
    hostname_header: Option<String>,
    app_name: String,
    proc_id: String,
    msg_id: String,
    /// The record header MSGID is read from, in place of the key and `msg_id`.
    msg_id_header: Option<String>,
    /// The STRUCTURED-DATA field, `-` when there is none.
    structured_data: String,
    /// The zone RFC 3164 timestamps are written in.
    zone: TimeZone,
}

impl Formatter {
    /// The formatter `config` names, header defaults filled in from the host.
    pub(super) fn new(config: &SyslogConfig, escape_lf: bool, zone: TimeZone) -> Self {
        let pri = config.facility as u8 * 8 + config.severity as u8;
        let hostname = config
            .hostname
            .clone()
            .or_else(|| sysinfo::System::host_name().ok())
            .unwrap_or_default();
        let app_name = config.app_name.clone().unwrap_or_else(executable_name);
        let proc_id = config
            .proc_id
            .clone()
            .unwrap_or_else(|| std::process::id().to_string());
        let app_max = match config.format {
            SyslogFormat::Rfc5424 => 48,
            // RFC 3164 caps the TAG at 32 alphanumerics.
            SyslogFormat::Rfc3164 => 32,
        };
        Self {
            format: config.format,
            pri,
            passthrough: config.passthrough,
            escape_lf,
            hostname: header(&hostname, HOSTNAME_MAX),
            hostname_header: config.hostname_header.clone(),
            app_name: match config.format {
                SyslogFormat::Rfc5424 => header(&app_name, app_max),
                SyslogFormat::Rfc3164 => tag(&app_name, app_max),
            },
            proc_id: header(&proc_id, 128),
            msg_id: header(config.msg_id.as_deref().unwrap_or(""), MSG_ID_MAX),
            msg_id_header: config.msg_id_header.clone(),
            structured_data: structured_data(config),
            zone,
        }
    }

    /// Append the message for `record` to `out`, stamped with its time, else
    /// now.
    pub(super) fn write(&self, record: &Record, out: &mut Vec<u8>) {
        let payload = trim_line_end(&record.payload);
        if self.passthrough && has_pri(payload) {
            self.write_body(payload, out);
            return;
        }
        let when = record_time(record).unwrap_or_else(Timestamp::now);
        // A write to a Vec cannot fail.
        match self.format {
            SyslogFormat::Rfc5424 => {
                let _ = write!(
                    out,
                    "<{}>1 {} ",
                    self.pri,
                    when.strftime("%Y-%m-%dT%H:%M:%S%.6fZ")
                );
                self.write_hostname(record, out);
                let _ = write!(out, " {} {} ", self.app_name, self.proc_id);
                self.write_msg_id(record, out);
                out.push(b' ');
                out.extend_from_slice(self.structured_data.as_bytes());
            }
            SyslogFormat::Rfc3164 => {
                let local = when.to_zoned_datetime(self.zone.clone());
                let _ = write!(out, "<{}>{} ", self.pri, local.strftime("%b %e %H:%M:%S"));
                self.write_hostname(record, out);
                let _ = write!(out, " {}[{}]:", self.app_name, self.proc_id);
            }
        }
        if !payload.is_empty() {
            out.push(b' ');
            self.write_body(payload, out);
        }
    }

    fn write_hostname(&self, record: &Record, out: &mut Vec<u8>) {
        match &self.hostname_header {
            Some(name) => write_field(header_value(record, name), HOSTNAME_MAX, out),
            None => out.extend_from_slice(self.hostname.as_bytes()),
        }
    }

    fn write_msg_id(&self, record: &Record, out: &mut Vec<u8>) {
        let key = record.key.as_deref().filter(|key| !key.is_empty());
        let value = match (&self.msg_id_header, key) {
            (Some(name), _) => header_value(record, name),
            (None, Some(key)) => key.as_bytes(),
            (None, None) => {
                out.extend_from_slice(self.msg_id.as_bytes());
                return;
            }
        };
        write_field(value, MSG_ID_MAX, out);
    }

    fn write_body(&self, body: &[u8], out: &mut Vec<u8>) {
        if !self.escape_lf {
            out.extend_from_slice(body);
            return;
        }
        for chunk in body.split_inclusive(|&b| b == b'\n') {
            match chunk.strip_suffix(b"\n") {
                Some(line) => {
                    out.extend_from_slice(line);
                    out.extend_from_slice(b"#012");
                }
                None => out.extend_from_slice(chunk),
            }
        }
    }
}

/// `payload` without one trailing `\n` or `\r\n`.
fn trim_line_end(payload: &[u8]) -> &[u8] {
    let payload = payload.strip_suffix(b"\n").unwrap_or(payload);
    payload.strip_suffix(b"\r").unwrap_or(payload)
}

/// Whether `payload` starts with a valid `<PRI>`: 1 to 3 digits, 0 to 191.
pub(super) fn has_pri(payload: &[u8]) -> bool {
    let Some(rest) = payload.strip_prefix(b"<") else {
        return false;
    };
    let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
    if !(1..=3).contains(&digits) || rest.get(digits) != Some(&b'>') {
        return false;
    }
    std::str::from_utf8(&rest[..digits])
        .ok()
        .and_then(|d| d.parse::<u16>().ok())
        .is_some_and(|pri| pri <= 191)
}

/// The time of `record`: its [`TIME_UNIX_NANO_HEADER`] floored to the
/// microsecond, else its `timestamp_ms`, `None` when neither is usable. RFC
/// 5424 has six fractional digits, so the floor loses nothing the wire can
/// hold.
fn record_time(record: &Record) -> Option<Timestamp> {
    let precise = parse_time_unix_nano(header_value(record, TIME_UNIX_NANO_HEADER))
        .and_then(|nanos| Timestamp::from_nanosecond(i128::from(nanos - nanos % 1000)).ok());
    precise.or_else(|| {
        record
            .metadata
            .timestamp_ms
            .and_then(|ms| Timestamp::from_millisecond(ms).ok())
    })
}

/// The value of `record`'s first header named `name`, empty when it has none.
fn header_value<'a>(record: &'a Record, name: &str) -> &'a [u8] {
    record
        .headers
        .iter()
        .find(|(key, _)| key == name)
        .map_or_default(|(_, value)| value.as_slice())
}

/// Append `value` as a header field: its printable ASCII with no spaces, at
/// most `max` of it, or `-` when that leaves nothing.
fn write_field(value: &[u8], max: usize, out: &mut Vec<u8>) {
    let begin = out.len();
    out.extend(value.iter().copied().filter(u8::is_ascii_graphic).take(max));
    if out.len() == begin {
        out.push(b'-');
    }
}

/// [`write_field`] into a string.
fn header(value: &str, max: usize) -> String {
    let mut field = Vec::with_capacity(value.len().min(max));
    write_field(value.as_bytes(), max, &mut field);
    String::from_utf8_lossy(&field).into_owned()
}

/// An RFC 3164 TAG: alphanumerics, `-`, `_` and `.`, at most `max` long.
fn tag(value: &str, max: usize) -> String {
    let field: String = value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(max)
        .collect();
    if field.is_empty() {
        "-".to_string()
    } else {
        field
    }
}

/// The STRUCTURED-DATA field of `config`, `-` when it has none. The config
/// has been validated, so names are SD-NAMEs.
fn structured_data(config: &SyslogConfig) -> String {
    if config.structured_data.is_empty() {
        return "-".to_string();
    }
    let mut out = String::new();
    for (id, params) in &config.structured_data {
        out.push('[');
        out.push_str(id);
        for (name, value) in params {
            out.push(' ');
            out.push_str(name);
            out.push_str("=\"");
            for c in value.chars() {
                if matches!(c, '"' | '\\' | ']') {
                    out.push('\\');
                }
                out.push(c);
            }
            out.push('"');
        }
        out.push(']');
    }
    out
}

/// The running executable's file stem, or empty when it cannot be read.
fn executable_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::syslog::config::{Facility, Severity};

    /// 2026-10-11T22:14:15.003Z
    const WHEN: i64 = 1_791_756_855_003;
    const STAMP: &str = "2026-10-11T22:14:15.003000Z";

    fn config(format: SyslogFormat) -> SyslogConfig {
        SyslogConfig {
            format,
            facility: Facility::Local4,
            severity: Severity::Notice,
            hostname: Some("web 01.example".into()),
            app_name: Some("billing".into()),
            proc_id: Some("4242".into()),
            ..SyslogConfig::default()
        }
    }

    /// A record of `payload` at [`WHEN`] with `key` and `headers`.
    fn record(payload: &[u8], key: Option<&str>, headers: &[(&str, &[u8])]) -> Record {
        Record {
            payload: bytes::Bytes::copy_from_slice(payload),
            key: key.map(std::sync::Arc::from),
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.to_vec()))
                .collect(),
            metadata: crate::transport::work_batch::RecordMeta {
                timestamp_ms: Some(WHEN),
                format: crate::transport::PayloadFormat::Json,
            },
        }
    }

    #[test]
    fn each_format_writes_its_header_before_the_payload() {
        let mut with_sd = config(SyslogFormat::Rfc5424);
        with_sd.structured_data.insert(
            "origin@32473".into(),
            [("note".to_string(), "a \"b\" ]c\\".to_string())].into(),
        );
        let mut passthrough = config(SyslogFormat::Rfc5424);
        passthrough.passthrough = true;
        let mut nothing = config(SyslogFormat::Rfc5424);
        nothing.hostname = Some(String::new());
        nothing.app_name = Some("\u{e9}".into());
        // (case, config, escape LF, payload, record key, wire)
        let cases: Vec<(&str, SyslogConfig, bool, &[u8], Option<&str>, &str)> = vec![
            (
                "5424",
                config(SyslogFormat::Rfc5424),
                false,
                b"{\"a\":1}\n",
                None,
                "<165>1 2026-10-11T22:14:15.003000Z web01.example billing 4242 - - {\"a\":1}",
            ),
            (
                "5424 key is the msgid",
                config(SyslogFormat::Rfc5424),
                false,
                b"x",
                Some("audit trail"),
                "<165>1 2026-10-11T22:14:15.003000Z web01.example billing 4242 audittrail - x",
            ),
            (
                "5424 structured data escaped",
                with_sd,
                false,
                b"x",
                None,
                "<165>1 2026-10-11T22:14:15.003000Z web01.example billing 4242 - \
                 [origin@32473 note=\"a \\\"b\\\" \\]c\\\\\"] x",
            ),
            (
                "3164",
                config(SyslogFormat::Rfc3164),
                false,
                b"started\r\n",
                Some("ignored"),
                "<165>Oct 11 22:14:15 web01.example billing[4242]: started",
            ),
            (
                "lf escaped",
                config(SyslogFormat::Rfc3164),
                true,
                b"one\ntwo\n",
                None,
                "<165>Oct 11 22:14:15 web01.example billing[4242]: one#012two",
            ),
            (
                "passthrough keeps a syslog line",
                passthrough.clone(),
                false,
                b"<13>Oct  1 00:00:00 h app: hi\n",
                None,
                "<13>Oct  1 00:00:00 h app: hi",
            ),
            (
                "passthrough formats anything else",
                passthrough,
                false,
                b"<999>no",
                None,
                "<165>1 2026-10-11T22:14:15.003000Z web01.example billing 4242 - - <999>no",
            ),
            (
                "empty fields are nil and an empty payload has no MSG",
                nothing,
                false,
                b"",
                None,
                "<165>1 2026-10-11T22:14:15.003000Z - - 4242 - -",
            ),
        ];
        for (case, config, escape, payload, key, want) in cases {
            let formatter = Formatter::new(&config, escape, TimeZone::UTC);
            let mut out = Vec::new();
            formatter.write(&record(payload, key, &[]), &mut out);
            assert_eq!(String::from_utf8_lossy(&out), want, "{case}");
        }
    }

    /// A `time_unix_nano` header is the RFC 5424 TIMESTAMP floored to the
    /// microsecond, never rounded, and wins over `timestamp_ms`. A header that
    /// is not decimal digits in a `u64` leaves `timestamp_ms` in force. RFC
    /// 3164 reads the same time to the second.
    #[test]
    fn a_time_header_sets_the_timestamp_floored_to_microseconds() {
        // (case, format, header value, wire TIMESTAMP)
        let cases: [(&str, SyslogFormat, Option<&[u8]>, &str); 11] = [
            (
                "sub-microsecond digits are dropped",
                SyslogFormat::Rfc5424,
                Some(b"1791756855003456789"),
                "2026-10-11T22:14:15.003456Z",
            ),
            (
                "999 ns past the microsecond floors",
                SyslogFormat::Rfc5424,
                Some(b"1791756855999999999"),
                "2026-10-11T22:14:15.999999Z",
            ),
            (
                "under one microsecond is zero",
                SyslogFormat::Rfc5424,
                Some(b"1791756855000000999"),
                "2026-10-11T22:14:15.000000Z",
            ),
            (
                "the header wins over timestamp_ms",
                SyslogFormat::Rfc5424,
                Some(b"1791756856500000000"),
                "2026-10-11T22:14:16.500000Z",
            ),
            (
                "surrounding spaces are trimmed",
                SyslogFormat::Rfc5424,
                Some(b" 1791756855003456789 "),
                "2026-10-11T22:14:15.003456Z",
            ),
            (
                "no header is timestamp_ms",
                SyslogFormat::Rfc5424,
                None,
                STAMP,
            ),
            ("an empty header", SyslogFormat::Rfc5424, Some(b""), STAMP),
            ("not a number", SyslogFormat::Rfc5424, Some(b"soon"), STAMP),
            ("negative", SyslogFormat::Rfc5424, Some(b"-1"), STAMP),
            (
                "a fraction",
                SyslogFormat::Rfc5424,
                Some(b"1791756855.5"),
                STAMP,
            ),
            (
                "3164 takes the header's second",
                SyslogFormat::Rfc3164,
                Some(b"1791756856999999999"),
                "Oct 11 22:14:16",
            ),
        ];
        for (case, format, value, stamp) in cases {
            let formatter = Formatter::new(&config(format), false, TimeZone::UTC);
            let headers: Vec<(&str, &[u8])> = value
                .map(|v| (TIME_UNIX_NANO_HEADER, v))
                .into_iter()
                .collect();
            let mut out = Vec::new();
            formatter.write(&record(b"x", None, &headers), &mut out);
            let wire = String::from_utf8_lossy(&out);
            let prefix = match format {
                SyslogFormat::Rfc5424 => "<165>1 ",
                SyslogFormat::Rfc3164 => "<165>",
            };
            let got = wire
                .strip_prefix(prefix)
                .and_then(|rest| rest.get(..stamp.len()));
            assert_eq!(got, Some(stamp), "{case}: {wire}");
        }
    }

    /// `hostname_header` and `msg_id_header` take HOSTNAME and MSGID from each
    /// record, sanitised as every header field is, and `-` where a record has
    /// nothing to give. Unset, a record's headers change nothing.
    #[test]
    fn header_keys_take_hostname_and_msgid_from_each_record() {
        let per_record = |format| SyslogConfig {
            hostname_header: Some("host.name".into()),
            msg_id_header: Some("source.kind".into()),
            proc_id: Some("-".into()),
            ..config(format)
        };
        let (long_host, long_kind) = ("h".repeat(300), "k".repeat(40));
        // (case, config, record key, record headers, wire)
        #[allow(clippy::type_complexity, reason = "a case table")]
        let cases: Vec<(&str, SyslogConfig, Option<&str>, Vec<(&str, &[u8])>, String)> = vec![
            (
                "unset, the headers change nothing",
                config(SyslogFormat::Rfc5424),
                None,
                vec![("host.name", b"other"), ("source.kind", b"other")],
                format!("<165>1 {STAMP} web01.example billing 4242 - - x"),
            ),
            (
                "from the headers, the key aside",
                per_record(SyslogFormat::Rfc5424),
                Some("audit"),
                vec![
                    ("host.name", b"\xffweb 02\x01.example"),
                    ("source.kind", b"file tail"),
                ],
                format!("<165>1 {STAMP} web02.example billing - filetail - x"),
            ),
            (
                "missing or no printable ASCII is nil, never the host name",
                per_record(SyslogFormat::Rfc5424),
                Some("audit"),
                vec![("source.kind", "\u{e9} ".as_bytes())],
                format!("<165>1 {STAMP} - billing - - - x"),
            ),
            (
                "cut to 255 and 32",
                per_record(SyslogFormat::Rfc5424),
                None,
                vec![
                    ("host.name", long_host.as_bytes()),
                    ("source.kind", long_kind.as_bytes()),
                ],
                format!(
                    "<165>1 {STAMP} {} billing - {} - x",
                    "h".repeat(255),
                    "k".repeat(32)
                ),
            ),
            (
                "3164 takes the hostname",
                per_record(SyslogFormat::Rfc3164),
                None,
                vec![("host.name", b"edge-7"), ("source.kind", b"unused")],
                "<165>Oct 11 22:14:15 edge-7 billing[-]: x".into(),
            ),
        ];
        for (case, config, key, headers, want) in cases {
            let formatter = Formatter::new(&config, false, TimeZone::UTC);
            let mut out = Vec::new();
            formatter.write(&record(b"x", key, &headers), &mut out);
            assert_eq!(String::from_utf8_lossy(&out), want, "{case}");
        }
    }

    #[test]
    fn defaults_fill_hostname_app_and_procid_from_the_host() {
        let formatter = Formatter::new(&SyslogConfig::default(), false, TimeZone::UTC);
        let mut out = Vec::new();
        let mut now = record(b"m", None, &[]);
        now.metadata.timestamp_ms = None;
        formatter.write(&now, &mut out);
        let text = String::from_utf8(out).unwrap();
        let fields: Vec<&str> = text.split(' ').collect();
        assert_eq!(fields[0], "<14>1", "user.info");
        assert_eq!(fields[4], std::process::id().to_string());
        assert_eq!(fields.last(), Some(&"m"));
    }

    #[test]
    fn a_pri_is_one_to_three_digits_up_to_191() {
        let cases: [(&[u8], bool); 8] = [
            (b"<0>", true),
            (b"<191>x", true),
            (b"<192>x", false),
            (b"<1234>x", false),
            (b"<>x", false),
            (b"<1x>", false),
            (b"13>", false),
            (b"", false),
        ];
        for (payload, want) in cases {
            assert_eq!(
                has_pri(payload),
                want,
                "{}",
                String::from_utf8_lossy(payload)
            );
        }
    }
}
