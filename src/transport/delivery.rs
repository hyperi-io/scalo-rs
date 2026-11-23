// Project:   scalo
// File:      src/transport/delivery.rs
// Purpose:   Delivery guarantee per sender and the delivery a config accepts
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! What a sender's `Ok` proves, and the weakest proof a config accepts.
//!
//! Every transport states a [`DeliveryGuarantee`] through
//! [`TransportBase::delivery_guarantee`](super::TransportBase::delivery_guarantee),
//! with no default, so a new backend cannot inherit a promise it does not keep.
//! The factory compares it with the config's `delivery` key, a
//! [`DeliveryRequirement`], and refuses a sender below it, naming the value
//! that would accept it.

use serde::{Deserialize, Serialize};

use super::error::TransportError;

/// What a sender's [`SendResult::Ok`](super::SendResult::Ok) proves about delivery.
///
/// Ordered weakest first, so `a < b` means `a` proves less.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    /// Handed to the operating system or the network with no flush and no
    /// answer: UDP, or Kafka with `acks=0`.
    FireAndForget,
    /// Written and flushed on this side, with no answer from the far end: a
    /// file, a pipe, a TCP stream.
    Written,
    /// The destination answered that it took the records: a broker ack, an
    /// HTTP status, a gRPC reply.
    Acknowledged,
}

impl DeliveryGuarantee {
    /// The name used in logs and config errors.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FireAndForget => "fire_and_forget",
            Self::Written => "written",
            Self::Acknowledged => "acknowledged",
        }
    }

    /// The weaker of two guarantees: what a sender reaching either one proves.
    #[must_use]
    pub fn weakest(self, other: Self) -> Self {
        self.min(other)
    }
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The weakest [`DeliveryGuarantee`] a factory-built sender may have: the
/// `delivery` key of a [`TransportConfig`](super::TransportConfig).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryRequirement {
    /// Only a sender the destination acknowledges. The default.
    #[default]
    Ack,
    /// A sender that writes and flushes is enough.
    Flush,
    /// Any sender, fire-and-forget included.
    None,
}

impl DeliveryRequirement {
    /// Whether a sender with `guarantee` meets this requirement.
    #[must_use]
    pub fn accepts(self, guarantee: DeliveryGuarantee) -> bool {
        guarantee >= self.floor()
    }

    /// The weakest guarantee this requirement accepts.
    #[must_use]
    pub fn floor(self) -> DeliveryGuarantee {
        match self {
            Self::Ack => DeliveryGuarantee::Acknowledged,
            Self::Flush => DeliveryGuarantee::Written,
            Self::None => DeliveryGuarantee::FireAndForget,
        }
    }

    /// The weakest requirement that accepts `guarantee`: the value to set.
    #[must_use]
    pub fn accepting(guarantee: DeliveryGuarantee) -> Self {
        match guarantee {
            DeliveryGuarantee::Acknowledged => Self::Ack,
            DeliveryGuarantee::Written => Self::Flush,
            DeliveryGuarantee::FireAndForget => Self::None,
        }
    }

    /// The config value, as written after `delivery:`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::Flush => "flush",
            Self::None => "none",
        }
    }

    /// `Ok` when `guarantee` meets this requirement, else the config error
    /// naming the `delivery` value that accepts it.
    ///
    /// `transport` names the sender, and `key` the cascade key its config was
    /// read from, when there is one, so the message names the full key.
    ///
    /// # Errors
    ///
    /// [`TransportError::Config`] when `guarantee` is below this requirement.
    pub fn check(
        self,
        transport: &str,
        guarantee: DeliveryGuarantee,
        key: Option<&str>,
    ) -> Result<(), TransportError> {
        if self.accepts(guarantee) {
            return Ok(());
        }
        let what = match guarantee {
            DeliveryGuarantee::Written => "writes and flushes but has no acknowledgement",
            DeliveryGuarantee::FireAndForget => "sends with no flush and no acknowledgement",
            DeliveryGuarantee::Acknowledged => "is acknowledged",
        };
        let setting = match key {
            Some(key) => format!("{key}.delivery"),
            None => "delivery".to_string(),
        };
        Err(TransportError::Config(format!(
            "{transport} transport {what}: set {setting}: {} to accept that (now {})",
            Self::accepting(guarantee),
            self
        )))
    }
}

impl std::fmt::Display for DeliveryRequirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use DeliveryGuarantee::{Acknowledged, FireAndForget, Written};

    #[test]
    fn each_requirement_accepts_its_floor_and_everything_stronger() {
        // (requirement, guarantee, accepted)
        let cases = [
            (DeliveryRequirement::Ack, Acknowledged, true),
            (DeliveryRequirement::Ack, Written, false),
            (DeliveryRequirement::Ack, FireAndForget, false),
            (DeliveryRequirement::Flush, Acknowledged, true),
            (DeliveryRequirement::Flush, Written, true),
            (DeliveryRequirement::Flush, FireAndForget, false),
            (DeliveryRequirement::None, Acknowledged, true),
            (DeliveryRequirement::None, Written, true),
            (DeliveryRequirement::None, FireAndForget, true),
        ];
        for (requirement, guarantee, accepted) in cases {
            assert_eq!(
                requirement.accepts(guarantee),
                accepted,
                "{requirement} accepting {guarantee}"
            );
            assert_eq!(
                requirement.check("x", guarantee, None).is_ok(),
                accepted,
                "{requirement} checking {guarantee}"
            );
        }
        assert_eq!(DeliveryRequirement::default(), DeliveryRequirement::Ack);
    }

    #[test]
    fn a_refusal_names_the_key_and_the_value_that_accepts_the_sender() {
        // (requirement, guarantee, key, text the error carries)
        let cases = [
            (
                DeliveryRequirement::Ack,
                Written,
                None,
                "file transport writes and flushes but has no acknowledgement: set delivery: flush to accept that",
            ),
            (
                DeliveryRequirement::Ack,
                Written,
                Some("transport.output"),
                "set transport.output.delivery: flush to accept that",
            ),
            (
                DeliveryRequirement::Ack,
                FireAndForget,
                None,
                "file transport sends with no flush and no acknowledgement: set delivery: none to accept that",
            ),
            (
                DeliveryRequirement::Flush,
                FireAndForget,
                None,
                "set delivery: none to accept that (now flush)",
            ),
        ];
        for (requirement, guarantee, key, text) in cases {
            let Err(TransportError::Config(message)) = requirement.check("file", guarantee, key)
            else {
                panic!("{requirement} must refuse {guarantee}");
            };
            assert!(message.contains(text), "{message}");
        }
    }

    #[test]
    fn the_config_values_round_trip() {
        for (text, requirement) in [
            ("ack", DeliveryRequirement::Ack),
            ("flush", DeliveryRequirement::Flush),
            ("none", DeliveryRequirement::None),
        ] {
            let parsed: DeliveryRequirement =
                serde_json::from_value(serde_json::Value::String(text.into())).unwrap();
            assert_eq!(parsed, requirement);
            assert_eq!(requirement.to_string(), text);
        }
        assert!(
            serde_json::from_value::<DeliveryRequirement>(serde_json::Value::String(
                "acked".into()
            ))
            .is_err(),
            "an unknown value is refused, never read as the default"
        );
        assert_eq!(
            Acknowledged.weakest(Written),
            Written,
            "the weaker guarantee wins"
        );
    }
}
