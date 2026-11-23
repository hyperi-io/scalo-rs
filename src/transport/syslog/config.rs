// Project:   scalo
// File:      src/transport/syslog/config.rs
// Purpose:   Syslog transport configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::tls::ClientTlsConfig;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::filter::FilterRule;

/// How messages reach the receiver.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum SyslogProtocol {
    /// A TCP stream, framed per `framing` (RFC 6587). Guarantee `written`.
    #[default]
    Tcp,
    /// TCP with TLS, framed per `framing` (RFC 5425). Guarantee `written`.
    Tls,
    /// One message per datagram (RFC 5426). Guarantee `fire_and_forget`.
    Udp,
    /// RELP: each message answered by the receiver. Guarantee `acknowledged`.
    /// TLS when a `tls` section is set.
    Relp,
}

impl SyslogProtocol {
    /// The config name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Tls => "tls",
            Self::Udp => "udp",
            Self::Relp => "relp",
        }
    }
}

/// Where one message ends on a TCP or TLS stream (RFC 6587).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SyslogFraming {
    /// `<length> <message>`. A message may hold any byte.
    #[default]
    OctetCounting,
    /// The message then `\n`. A `\n` inside a message is sent as `#012`.
    Lf,
}

/// The header written before each record's payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum SyslogFormat {
    /// `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID SD MSG`, the time in UTC.
    #[default]
    Rfc5424,
    /// `<PRI>Mmm dd hh:mm:ss HOSTNAME TAG[PROCID]: MSG`, the time in the host's zone.
    Rfc3164,
}

/// The syslog facility, by its RFC 5424 keyword.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[allow(
    missing_docs,
    reason = "each variant is the RFC 5424 facility of its name"
)]
pub enum Facility {
    Kern = 0,
    #[default]
    User = 1,
    Mail = 2,
    Daemon = 3,
    Auth = 4,
    Syslog = 5,
    Lpr = 6,
    News = 7,
    Uucp = 8,
    Cron = 9,
    Authpriv = 10,
    Ftp = 11,
    Ntp = 12,
    Audit = 13,
    Alert = 14,
    Clock = 15,
    Local0 = 16,
    Local1 = 17,
    Local2 = 18,
    Local3 = 19,
    Local4 = 20,
    Local5 = 21,
    Local6 = 22,
    Local7 = 23,
}

/// The syslog severity, by its RFC 5424 keyword.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[allow(
    missing_docs,
    reason = "each variant is the RFC 5424 severity of its name"
)]
pub enum Severity {
    #[serde(alias = "emergency")]
    Emerg = 0,
    Alert = 1,
    #[serde(alias = "critical")]
    Crit = 2,
    #[serde(alias = "error")]
    Err = 3,
    #[serde(alias = "warn")]
    Warning = 4,
    Notice = 5,
    #[default]
    #[serde(alias = "informational")]
    Info = 6,
    Debug = 7,
}

/// Syslog transport configuration, read from `transport.syslog` or from the
/// `syslog` section of a [`TransportConfig`](crate::transport::TransportConfig).
///
/// ```yaml
/// transport:
///   output:
///     type: syslog
///     delivery: flush        # tcp and tls write and flush, no answer
///     syslog:
///       address: logs.example.net:6514
///       protocol: tls
///       facility: local0
///       app_name: billing
///       tls:
///         trust: bundle
///         ca_path: /etc/tls/ca.pem
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct SyslogConfig {
    /// The receiver, `host:port` or `[v6]:port`. Required.
    pub address: String,
    /// `tcp` (default), `tls`, `udp` or `relp`.
    pub protocol: SyslogProtocol,
    /// TCP and TLS stream framing: `octet_counting` (default) or `lf`.
    pub framing: SyslogFraming,
    /// `rfc5424` (default) or `rfc3164`.
    pub format: SyslogFormat,
    /// Send a payload that already starts with a `<PRI>` as it is, with no
    /// header added. Default false.
    pub passthrough: bool,
    /// Default `user`.
    pub facility: Facility,
    /// Default `info`.
    pub severity: Severity,
    /// HOSTNAME. Default the machine's host name.
    pub hostname: Option<String>,
    /// APP-NAME, the TAG under RFC 3164. Default the executable's name.
    pub app_name: Option<String>,
    /// PROCID. Default the process id.
    pub proc_id: Option<String>,
    /// MSGID for a record with no key. A record's key is its MSGID. Default `-`.
    pub msg_id: Option<String>,
    /// RFC 5424 STRUCTURED-DATA on every message: SD-ID to its parameters.
    pub structured_data: BTreeMap<String, BTreeMap<String, String>>,
    /// The largest message, header included. UDP cuts a longer one, every
    /// other protocol dead-letters it. Default 2048 for UDP, 8192 otherwise.
    pub max_message_bytes: Option<usize>,
    /// TCP connect and TLS or RELP handshake bound, in ms. Default 5000.
    pub connect_timeout_ms: u64,
    /// Bound on one send with no deadline of its own, in ms. Default 30000.
    pub send_timeout_ms: u64,
    /// RELP messages sent before an answer is awaited. Default 128.
    pub relp_window: usize,
    /// Client TLS, for `tls`, and for `relp` over TLS. Default trust `native`.
    pub tls: Option<SyslogTlsConfig>,
    /// Proxy for `tcp`, `tls` and `relp`: `none` (default), `system` or a
    /// proxy URL. The connection is an HTTP CONNECT tunnel, and TLS runs
    /// inside it. `udp` takes `none` only. See
    /// [`ProxyConfig`](crate::transport::ProxyConfig).
    pub proxy: crate::transport::ProxyConfig,
    /// Outbound message filters, applied to each record's payload.
    pub filters_out: Vec<FilterRule>,
}

/// Client TLS for syslog: the shared [`ClientTlsConfig`] keys plus the name
/// the receiver's certificate is checked against.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct SyslogTlsConfig {
    /// Trust, CA file and client certificate.
    #[serde(flatten)]
    pub client: ClientTlsConfig,
    /// The name checked against the receiver's certificate. Default the host
    /// part of `address`.
    pub server_name: Option<String>,
}

/// Datagrams over this cannot be sent over IPv4 or IPv6.
pub(super) const MAX_UDP_PAYLOAD: usize = 65_507;

/// The shortest `max_message_bytes` a receiver must take (RFC 5426).
const MIN_MESSAGE_BYTES: usize = 480;

impl Default for SyslogConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            protocol: SyslogProtocol::Tcp,
            framing: SyslogFraming::OctetCounting,
            format: SyslogFormat::Rfc5424,
            passthrough: false,
            facility: Facility::User,
            severity: Severity::Info,
            hostname: None,
            app_name: None,
            proc_id: None,
            msg_id: None,
            structured_data: BTreeMap::new(),
            max_message_bytes: None,
            connect_timeout_ms: 5_000,
            send_timeout_ms: 30_000,
            relp_window: 128,
            tls: None,
            proxy: crate::transport::ProxyConfig::None,
            filters_out: Vec::new(),
        }
    }
}

impl crate::transport::traits::FromCascade for SyslogConfig {}

impl SyslogConfig {
    /// Load from the config cascade under `transport.syslog`, or the defaults
    /// when it is absent.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the section is present but does not parse.
    pub fn from_cascade() -> TransportResult<Self> {
        <Self as crate::transport::traits::FromCascade>::from_cascade_key("transport.syslog")
    }

    /// A config sending to `address` over `protocol`.
    #[must_use]
    pub fn new(address: impl Into<String>, protocol: SyslogProtocol) -> Self {
        Self {
            address: address.into(),
            protocol,
            ..Self::default()
        }
    }

    /// `max_message_bytes`, or the protocol's default.
    #[must_use]
    pub fn message_limit(&self) -> usize {
        self.max_message_bytes.unwrap_or(match self.protocol {
            SyslogProtocol::Udp => 2048,
            _ => 8192,
        })
    }

    /// Whether this config sends over TLS.
    #[must_use]
    pub fn uses_tls(&self) -> bool {
        match self.protocol {
            SyslogProtocol::Tls => true,
            SyslogProtocol::Relp => self.tls.is_some(),
            SyslogProtocol::Tcp | SyslogProtocol::Udp => false,
        }
    }

    /// Refuse a config that cannot send.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` naming the key at fault.
    pub(super) fn validate(&self) -> TransportResult<()> {
        let fail = |what: String| Err(TransportError::Config(format!("syslog: {what}")));
        if host_of(&self.address).is_none() {
            return fail(format!(
                "address `{}` is not host:port or [v6]:port",
                self.address
            ));
        }
        if self.tls.is_some() && matches!(self.protocol, SyslogProtocol::Tcp | SyslogProtocol::Udp)
        {
            return fail(format!(
                "a tls section is set but protocol is {}: use protocol tls, or relp for RELP over TLS",
                self.protocol.as_str()
            ));
        }
        if self.protocol == SyslogProtocol::Udp {
            self.proxy.refuse_for_udp("syslog")?;
        }
        let limit = self.message_limit();
        if limit < MIN_MESSAGE_BYTES {
            return fail(format!(
                "max_message_bytes {limit} is under {MIN_MESSAGE_BYTES}, the least a receiver takes"
            ));
        }
        if self.protocol == SyslogProtocol::Udp && limit > MAX_UDP_PAYLOAD {
            return fail(format!(
                "max_message_bytes {limit} is over {MAX_UDP_PAYLOAD}, the largest UDP datagram"
            ));
        }
        if self.relp_window == 0 {
            return fail("relp_window must be at least 1".to_string());
        }
        if self.connect_timeout_ms == 0 || self.send_timeout_ms == 0 {
            return fail("connect_timeout_ms and send_timeout_ms must be above 0".to_string());
        }
        for (id, params) in &self.structured_data {
            if !is_sd_name(id) {
                return fail(format!(
                    "structured_data id `{id}` must be 1 to 32 printable ASCII characters \
                     with no space, `=`, `]` or `\"`"
                ));
            }
            if let Some(name) = params.keys().find(|name| !is_sd_name(name)) {
                return fail(format!(
                    "structured_data `{id}` parameter `{name}` must be 1 to 32 printable \
                     ASCII characters with no space, `=`, `]` or `\"`"
                ));
            }
        }
        Ok(())
    }
}

/// An RFC 5424 SD-NAME: 1 to 32 printable ASCII characters, none of them a
/// space, `=`, `]` or `"`.
fn is_sd_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_graphic() && !matches!(b, b'=' | b']' | b'"'))
}

/// The port of an address [`host_of`] takes.
pub(super) fn port_of(address: &str) -> Option<u16> {
    host_of(address)?;
    address.rsplit_once(':')?.1.parse().ok()
}

/// The host part of `host:port` or `[v6]:port`.
pub(super) fn host_of(address: &str) -> Option<&str> {
    if let Some(rest) = address.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        return port.parse::<u16>().ok().map(|_| host);
    }
    let (host, port) = address.rsplit_once(':')?;
    (!host.is_empty() && !host.contains(':') && port.parse::<u16>().is_ok()).then_some(host)
}
