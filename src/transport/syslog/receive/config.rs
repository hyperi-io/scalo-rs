// Project:   scalo
// File:      src/transport/syslog/receive/config.rs
// Purpose:   Syslog listener configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::framing::{CarriageReturns, DecodeLimits};
use crate::tls_config::ServerTlsConfig;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::filter::FilterRule;

/// The syslog listeners, read from `transport.syslog.listen` or from the
/// `listen` section of a [`SyslogConfig`](super::super::SyslogConfig).
///
/// ```yaml
/// transport:
///   input:
///     type: syslog
///     syslog:
///       listen:
///         tcp_address: 0.0.0.0:514
///         udp_address: 0.0.0.0:514
///         tls_address: 0.0.0.0:6514
///         tls:
///           cert_file: /etc/tls/syslog.pem
///           key_file: /etc/tls/syslog.key
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct SyslogListenConfig {
    /// TCP listen address, `ip:port`. Unset, no TCP listener.
    pub tcp_address: Option<String>,
    /// TLS listen address (RFC 5425), served with `tls`. Unset, no TLS listener.
    pub tls_address: Option<String>,
    /// UDP listen address (RFC 5426). Unset, no UDP listener.
    pub udp_address: Option<String>,
    /// Server TLS for `tls_address`, the one `tls` section: profile,
    /// certificate, key, client CAs, client auth and reload.
    pub tls: Option<ServerTlsConfig>,
    /// Largest message on TCP and TLS, in bytes. A longer one is skipped and
    /// counted, and the connection carries on. Default 65536.
    pub max_message_bytes: usize,
    /// Largest octet count trusted on TCP and TLS. A message over
    /// `max_message_bytes` with a count up to this is skipped by its count, and
    /// a larger count is read as an LF line. Read as at least
    /// `max_message_bytes`. Default 262144.
    pub max_frame_bytes: usize,
    /// What an LF line keeps of the `\r`s before its `\n`: `strip` (default)
    /// or `keep`.
    pub carriage_returns: CarriageReturns,
    /// Seconds a TCP or TLS connection may be quiet before keepalive probes
    /// start, one probe per this interval. 0 turns keepalive off. Default 60.
    pub tcp_keepalive_secs: u64,
    /// A TCP or TLS connection with no frame for this long, in ms, is closed.
    /// 0 keeps it. Default 600000.
    pub idle_timeout_ms: u64,
    /// Once a frame's first byte arrives, the rest has this long, in ms, or the
    /// connection is closed. 0 waits for ever. Default 30000.
    pub frame_timeout_ms: u64,
    /// UDP socket receive buffer (`SO_RCVBUF`), in bytes. The kernel may grant
    /// less, and the listener logs what it got. Default 8 MiB.
    pub udp_recv_buffer_bytes: usize,
    /// Most datagrams read in one wake. Default 64.
    pub udp_batch_size: usize,
    /// Records queued for `recv`. A full queue stops TCP and TLS reads, and
    /// drops datagrams, counted. Default 10000.
    pub recv_buffer_size: usize,
    /// How long `recv` waits for a first record, in ms. Default 100.
    pub recv_timeout_ms: u64,
    /// Inbound message filters, applied on `recv`.
    pub filters_in: Vec<FilterRule>,
}

impl Default for SyslogListenConfig {
    fn default() -> Self {
        Self {
            tcp_address: None,
            tls_address: None,
            udp_address: None,
            tls: None,
            max_message_bytes: 64 * 1024,
            max_frame_bytes: 256 * 1024,
            carriage_returns: CarriageReturns::Strip,
            tcp_keepalive_secs: 60,
            idle_timeout_ms: 600_000,
            frame_timeout_ms: 30_000,
            udp_recv_buffer_bytes: 8 * 1024 * 1024,
            udp_batch_size: 64,
            recv_buffer_size: 10_000,
            recv_timeout_ms: 100,
            filters_in: Vec::new(),
        }
    }
}

impl crate::transport::traits::FromCascade for SyslogListenConfig {}

/// The addresses a valid config binds.
#[derive(Debug, Default)]
pub(super) struct Addresses {
    pub(super) tcp: Option<SocketAddr>,
    pub(super) tls: Option<SocketAddr>,
    pub(super) udp: Option<SocketAddr>,
}

impl SyslogListenConfig {
    /// Load from the config cascade under `transport.syslog.listen`, or the
    /// defaults when it is absent.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the section is present but does not parse.
    pub fn from_cascade() -> TransportResult<Self> {
        <Self as crate::transport::traits::FromCascade>::from_cascade_key("transport.syslog.listen")
    }

    /// The limits each TCP or TLS message is framed under.
    #[must_use]
    pub fn limits(&self) -> DecodeLimits {
        DecodeLimits {
            max_decoded_bytes: usize::MAX,
            max_record_bytes: self.max_message_bytes,
            max_frame_bytes: self.max_frame_bytes,
        }
    }

    /// The addresses to bind.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` naming the key at fault.
    pub(super) fn addresses(&self) -> TransportResult<Addresses> {
        let fail = |what: &str| Err(TransportError::Config(format!("syslog listen: {what}")));
        let parse = |key: &str, address: &Option<String>| -> TransportResult<Option<SocketAddr>> {
            address
                .as_deref()
                .map(|a| {
                    a.parse().map_err(|e| {
                        TransportError::Config(format!(
                            "syslog listen: {key} `{a}` is not ip:port: {e}"
                        ))
                    })
                })
                .transpose()
        };
        let addresses = Addresses {
            tcp: parse("tcp_address", &self.tcp_address)?,
            tls: parse("tls_address", &self.tls_address)?,
            udp: parse("udp_address", &self.udp_address)?,
        };
        if addresses.tcp.is_none() && addresses.tls.is_none() && addresses.udp.is_none() {
            return fail("set tcp_address, tls_address or udp_address");
        }
        if addresses.tls.is_some() != self.tls.is_some() {
            return fail("tls_address and the tls section go together");
        }
        if self.max_message_bytes == 0 || self.recv_buffer_size == 0 || self.udp_batch_size == 0 {
            return fail("max_message_bytes, recv_buffer_size and udp_batch_size must be above 0");
        }
        Ok(addresses)
    }
}

/// `ms` as a duration, `None` for 0.
pub(super) fn millis(ms: u64) -> Option<Duration> {
    (ms > 0).then(|| Duration::from_millis(ms))
}
