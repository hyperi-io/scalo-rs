// Project:   scalo
// File:      src/yaml/de.rs
// Purpose:   Opt-in serde field helpers for YAML a generator may write sparsely
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Field helpers for `#[serde(deserialize_with = "...")]`.

use serde::{Deserialize, Deserializer};

/// Read a `String` field written as `key:` or `key: ~` as empty text.
///
/// A null into a plain `String` field is an error by default, so a field a
/// config generator may leave blank opts in here. Pair it with
/// `#[serde(default)]` so a missing key is also empty rather than an error.
///
/// ```rust
/// #[derive(serde::Deserialize)]
/// struct Kafka {
///     #[serde(default, deserialize_with = "scalo::yaml::de::null_as_empty")]
///     username: String,
/// }
///
/// let kafka: Kafka = scalo::yaml::from_str("username:\n", "kafka.yaml", scalo::yaml::Profile::Config)?;
/// assert_eq!(kafka.username, "");
/// # Ok::<(), scalo::yaml::YamlError>(())
/// ```
///
/// # Errors
///
/// The deserializer's error when the value is neither text nor null.
pub fn null_as_empty<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Option::<String>::deserialize(deserializer).map(Option::unwrap_or_default)
}
