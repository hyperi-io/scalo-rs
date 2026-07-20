// Project:   scalo
// File:      src/transport/kafka/contract.rs
// Purpose:   Opt-in STRICT Kafka credential profile - allow-list providers and
//            refuse any weakening of a provider's strongest auth.
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! An opinionated, OPT-IN strict credential profile layered on top of the vanilla
//! [`super::providers::KnownProvider`] facts.
//!
//! The vanilla provider abstraction states FACTS (what each provider's platform
//! requires). This module adds POLICY: restrict to an allow-list of blessed
//! providers, and refuse a hand-set config that WEAKENS a provider's strongest
//! auth (e.g. PLAIN on a broker that supports SCRAM). It is opt-in -- the vanilla
//! providers work without importing it.
//!
//! This encodes the DFE credential contract (dfe-engine#98) when driven with DFE's
//! allow-list, but the shape is generic: any security-conscious operator wants
//! "strongest available, never downgraded, only these providers".

use super::providers::{KafkaProvider, KnownProvider, validate};

/// The default allow-list of blessed providers. `msk_iam` is deliberately absent --
/// it is the quarantined IAM path, not on the username+password contract.
pub const DEFAULT_ALLOWED: &[&str] = &[
    "strimzi",
    "redpanda",
    "msk",
    "redpanda-cloud",
    "confluent-cloud",
    "plaintext",
];

/// Derive a provider's auth, refusing any provider outside `allowed`.
///
/// # Errors
/// Returns `Err` if the provider is not in `allowed`, or is unknown.
pub fn require(provider: &str, allowed: &[&str]) -> Result<(&'static str, &'static str), String> {
    if !allowed.contains(&provider) {
        return Err(format!(
            "provider {provider:?} is not in the allowed set {allowed:?}"
        ));
    }
    Ok(KnownProvider::parse(provider)?.auth())
}

/// Derive a provider's auth against the [`DEFAULT_ALLOWED`] blessed set.
///
/// # Errors
/// Returns `Err` if the provider is not blessed, or is unknown.
pub fn require_blessed(provider: &str) -> Result<(&'static str, &'static str), String> {
    require(provider, DEFAULT_ALLOWED)
}

/// Refuse a config that WEAKENS a provider's strongest auth. Also enforces the
/// universal security floor (via the vanilla [`validate`]).
///
/// # Errors
/// Returns `Err` if the passed `(security_protocol, sasl_mechanism)` is not the
/// provider's strongest, or breaks the floor, or the provider is unknown.
pub fn assert_not_weakened(
    provider: &str,
    security_protocol: &str,
    sasl_mechanism: &str,
) -> Result<(), String> {
    validate(security_protocol, sasl_mechanism)?;
    let (want_proto, want_mech) = KnownProvider::parse(provider)?.auth();
    if !security_protocol.eq_ignore_ascii_case(want_proto) || sasl_mechanism != want_mech {
        return Err(format!(
            "config for provider {provider:?} weakens its auth: got \
             ({security_protocol}, {sasl_mechanism}); the provider's strongest is \
             ({want_proto}, {want_mech})"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_blessed_returns_derived_auth() {
        assert_eq!(
            require_blessed("confluent-cloud").unwrap(),
            ("SASL_SSL", "PLAIN")
        );
        assert_eq!(
            require_blessed("strimzi").unwrap(),
            ("SASL_SSL", "SCRAM-SHA-512")
        );
    }

    #[test]
    fn require_refuses_quarantined_and_unknown() {
        // msk_iam is a known provider but NOT blessed (quarantined IAM path).
        assert!(require_blessed("msk_iam").is_err());
        assert!(require_blessed("kinesis").is_err());
    }

    #[test]
    fn require_honours_a_custom_allow_list() {
        assert!(require("redpanda", &["redpanda"]).is_ok());
        assert!(require("confluent-cloud", &["redpanda"]).is_err());
    }

    #[test]
    fn assert_not_weakened_accepts_the_strongest() {
        assert!(assert_not_weakened("strimzi", "SASL_SSL", "SCRAM-SHA-512").is_ok());
        // Case-insensitive on the protocol (scalo stores it lowercased).
        assert!(assert_not_weakened("strimzi", "sasl_ssl", "SCRAM-SHA-512").is_ok());
        assert!(assert_not_weakened("confluent-cloud", "SASL_SSL", "PLAIN").is_ok());
    }

    #[test]
    fn assert_not_weakened_refuses_a_downgrade() {
        // PLAIN on an owned broker that supports SCRAM -- a weakening.
        assert!(assert_not_weakened("strimzi", "SASL_SSL", "PLAIN").is_err());
        // Even though PLAIN-over-SASL_SSL passes the floor, it is weaker than the
        // provider's SCRAM, so the strict profile refuses it.
    }
}
