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

/// Writes one record as one syslog message.
#[derive(Debug, Clone)]
pub(super) struct Formatter {
    format: SyslogFormat,
    pri: u8,
    passthrough: bool,
    /// Send an LF inside a message as `#012`, for LF framing.
    escape_lf: bool,
    hostname: String,
    app_name: String,
    proc_id: String,
    msg_id: String,
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
        let (host_max, app_max) = match config.format {
            SyslogFormat::Rfc5424 => (255, 48),
            // RFC 3164 caps the TAG at 32 alphanumerics.
            SyslogFormat::Rfc3164 => (255, 32),
        };
        Self {
            format: config.format,
            pri,
            passthrough: config.passthrough,
            escape_lf,
            hostname: header(&hostname, host_max),
            app_name: match config.format {
                SyslogFormat::Rfc5424 => header(&app_name, app_max),
                SyslogFormat::Rfc3164 => tag(&app_name, app_max),
            },
            proc_id: header(&proc_id, 128),
            msg_id: header(config.msg_id.as_deref().unwrap_or(""), 32),
            structured_data: structured_data(config),
            zone,
        }
    }

    /// Append the message for `payload` to `out`. `msg_id` replaces the
    /// configured MSGID when set, and `millis` is the record's time, else now.
    pub(super) fn write(
        &self,
        payload: &[u8],
        msg_id: Option<&str>,
        millis: Option<i64>,
        out: &mut Vec<u8>,
    ) {
        let payload = trim_line_end(payload);
        if self.passthrough && has_pri(payload) {
            self.write_body(payload, out);
            return;
        }
        let when = millis
            .and_then(|ms| Timestamp::from_millisecond(ms).ok())
            .unwrap_or_else(Timestamp::now);
        // A write to a Vec cannot fail.
        let _ = match self.format {
            SyslogFormat::Rfc5424 => {
                let msg_id = msg_id
                    .filter(|id| !id.is_empty())
                    .map_or_else(|| self.msg_id.clone(), |id| header(id, 32));
                write!(
                    out,
                    "<{}>1 {} {} {} {} {} {}",
                    self.pri,
                    when.strftime("%Y-%m-%dT%H:%M:%S%.6fZ"),
                    self.hostname,
                    self.app_name,
                    self.proc_id,
                    msg_id,
                    self.structured_data,
                )
            }
            SyslogFormat::Rfc3164 => {
                let local = when.to_zoned_datetime(self.zone.clone());
                write!(
                    out,
                    "<{}>{} {} {}[{}]:",
                    self.pri,
                    local.strftime("%b %e %H:%M:%S"),
                    self.hostname,
                    self.app_name,
                    self.proc_id,
                )
            }
        };
        if !payload.is_empty() {
            out.push(b' ');
            self.write_body(payload, out);
        }
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

/// A header field: printable ASCII with no spaces, at most `max` long, `-`
/// when empty.
fn header(value: &str, max: usize) -> String {
    let field: String = value
        .chars()
        .filter(char::is_ascii_graphic)
        .take(max)
        .collect();
    if field.is_empty() {
        "-".to_string()
    } else {
        field
    }
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
            formatter.write(payload, key, Some(WHEN), &mut out);
            assert_eq!(String::from_utf8_lossy(&out), want, "{case}");
        }
    }

    #[test]
    fn defaults_fill_hostname_app_and_procid_from_the_host() {
        let formatter = Formatter::new(&SyslogConfig::default(), false, TimeZone::UTC);
        let mut out = Vec::new();
        formatter.write(b"m", None, None, &mut out);
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
