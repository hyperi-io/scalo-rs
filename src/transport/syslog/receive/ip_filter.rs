// Project:   scalo
// File:      src/transport/syslog/receive/ip_filter.rs
// Purpose:   Syslog listener IP filter: allowlist or denylist of CIDRs
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::net::IpAddr;

use ipnet::IpNet;
use ipnet_trie::IpnetTrie;
use serde::{Deserialize, Serialize};

use crate::transport::error::{TransportError, TransportResult};

/// Which senders the syslog listeners take traffic from.
///
/// ```yaml
/// ip_filter:
///   mode: allowlist
///   cidrs: ["10.0.0.0/8", "fd00::/8"]
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct IpFilterConfig {
    /// `disabled` (default), `allowlist` or `denylist`.
    pub mode: IpFilterMode,
    /// IPv4 and IPv6 CIDRs, such as `10.0.0.0/8` or `fd00::/8`.
    pub cidrs: Vec<String>,
}

/// How [`IpFilterConfig::cidrs`] is read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum IpFilterMode {
    /// Every sender is taken, and `cidrs` is not read.
    #[default]
    Disabled,
    /// Only a sender inside one of `cidrs` is taken.
    Allowlist,
    /// Every sender but those inside one of `cidrs` is taken.
    Denylist,
}

/// [`IpFilterConfig`] compiled into a prefix trie, one lookup per connection
/// or datagram.
pub(super) struct IpFilter {
    /// The mode and its CIDRs, `None` when disabled.
    rule: Option<(IpFilterMode, IpnetTrie<()>)>,
}

impl IpFilter {
    /// Compile `config`.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for an entry that is not a CIDR, and for an
    /// allowlist with none, which would take no sender.
    pub(super) fn new(config: &IpFilterConfig) -> TransportResult<Self> {
        let fail =
            |what: String| TransportError::Config(format!("syslog listen: ip_filter: {what}"));
        let mut trie = IpnetTrie::new();
        for cidr in &config.cidrs {
            let net: IpNet = cidr
                .trim()
                .parse()
                .map_err(|e| fail(format!("`{cidr}` is not a CIDR: {e}")))?;
            trie.insert(net, ());
        }
        let rule = match config.mode {
            IpFilterMode::Disabled => None,
            IpFilterMode::Allowlist if config.cidrs.is_empty() => {
                return Err(fail(
                    "an allowlist with no cidrs takes no sender: list them, or set mode: disabled"
                        .into(),
                ));
            }
            mode => Some((mode, trie)),
        };
        Ok(Self { rule })
    }

    /// Whether traffic from `ip` is taken. An IPv4-mapped IPv6 address, as a
    /// dual-stack listener sees an IPv4 sender, is checked as the IPv4 one.
    #[must_use]
    pub(super) fn admits(&self, ip: IpAddr) -> bool {
        let Some((mode, trie)) = &self.rule else {
            return true;
        };
        let listed = trie
            .longest_match(&IpNet::from(ip.to_canonical()))
            .is_some();
        listed == (*mode == IpFilterMode::Allowlist)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(mode: IpFilterMode, cidrs: &[&str]) -> TransportResult<IpFilter> {
        IpFilter::new(&IpFilterConfig {
            mode,
            cidrs: cidrs.iter().map(ToString::to_string).collect(),
        })
    }

    /// Each mode takes the senders it should, IPv4 and IPv6, an IPv4-mapped
    /// IPv6 sender checked as its IPv4 address.
    #[test]
    fn each_mode_takes_the_senders_it_lists() {
        let cidrs = ["10.0.0.0/8", "fd00::/8", " 192.0.2.7/32 "];
        // (sender, taken by allowlist)
        let senders = [
            ("10.1.2.3", true),
            ("192.0.2.7", true),
            ("192.0.2.8", false),
            ("::ffff:10.9.9.9", true),
            ("::ffff:11.0.0.1", false),
            ("fd12::1", true),
            ("2001:db8::1", false),
            ("127.0.0.1", false),
        ];
        let allow = filter(IpFilterMode::Allowlist, &cidrs).unwrap();
        let deny = filter(IpFilterMode::Denylist, &cidrs).unwrap();
        let off = filter(IpFilterMode::Disabled, &cidrs).unwrap();
        for (sender, listed) in senders {
            let ip: IpAddr = sender.parse().unwrap();
            assert_eq!(
                (allow.admits(ip), deny.admits(ip), off.admits(ip)),
                (listed, !listed, true),
                "{sender}"
            );
        }
    }

    /// An entry that is not a CIDR, and an allowlist with none, are refused
    /// naming the fault. A disabled filter with no CIDRs is fine.
    #[test]
    fn a_filter_that_cannot_work_is_refused() {
        // (mode, cidrs, error text, or None when it builds)
        let cases: [(IpFilterMode, &[&str], Option<&str>); 5] = [
            (
                IpFilterMode::Allowlist,
                &[],
                Some("an allowlist with no cidrs"),
            ),
            (
                IpFilterMode::Allowlist,
                &["10.0.0/8"],
                Some("`10.0.0/8` is not a CIDR"),
            ),
            (
                IpFilterMode::Denylist,
                &["10.0.0.1"],
                Some("`10.0.0.1` is not a CIDR"),
            ),
            (IpFilterMode::Denylist, &[], None),
            (IpFilterMode::Disabled, &[], None),
        ];
        for (mode, cidrs, want) in cases {
            let got = filter(mode, cidrs).err().map(|e| e.to_string());
            match want {
                Some(want) => assert!(
                    got.as_deref().is_some_and(|e| e.contains(want)),
                    "{mode:?} {cidrs:?}: {got:?}"
                ),
                None => assert_eq!(got, None, "{mode:?} {cidrs:?}"),
            }
        }
    }
}
