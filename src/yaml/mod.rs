// Project:   scalo
// File:      src/yaml/mod.rs
// Purpose:   YAML parse and emit with scalo's options, budgets and error reporting
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! YAML parse and emit for every scalo consumer.
//!
//! One set of parse rules, applied the same way to config files, operator data
//! and YAML a service writes. Parsing is [serde-saphyr](https://crates.io/crates/serde-saphyr),
//! which is safe Rust throughout.
//!
//! ## Parse rules
//!
//! | Input | Result |
//! | --- | --- |
//! | `yes`, `no`, `on`, `off` | Text. Only `true` and `false` are booleans |
//! | `key:` or `key: ~` into a `String` field | An error at that line. `Option<String>` and [`SensitiveString`](crate::SensitiveString) read it as empty. [`de::null_as_empty`] opts a `String` field in |
//! | A key repeated in one mapping | An error |
//! | `<<: *anchor` merge keys | Merged into the mapping |
//! | `.inf`, `.nan` where any type is accepted | An error. A typed `f64` field still reads them |
//! | `!!binary` into a `String` field | The text as written |
//! | `0123`, `1_000` where any type is accepted | The numbers `123` and `1000`. A typed `String` field keeps the text. [`from_str_reporting`] lints them |
//! | More than one document | An error |
//!
//! "Where any type is accepted" means `serde_json::Value`, untagged and
//! internally tagged enums, `#[serde(flatten)]` maps and the config cascade.
//!
//! ## Profiles
//!
//! A [`Profile`] says what the YAML is, and sets the parse budget and the lint
//! passes. Errors never quote a neighbouring line, so a secret on the line
//! above a mistake stays out of logs.
//!
//! ```rust
//! use scalo::yaml::{self, Profile};
//!
//! #[derive(Debug, serde::Deserialize)]
//! struct Kafka {
//!     brokers: Vec<String>,
//!     enabled: bool,
//! }
//!
//! let kafka: Kafka = yaml::from_str("brokers: ['kafka:9092']\nenabled: true\n", "kafka.yaml", Profile::Config)?;
//! assert!(kafka.enabled);
//!
//! let err = yaml::from_str::<Kafka>("brokers: []\nenabled: yes\n", "kafka.yaml", Profile::Config)
//!     .unwrap_err();
//! assert_eq!(err.line, Some(2));
//! # Ok::<(), scalo::yaml::YamlError>(())
//! ```

pub mod de;
mod error;
mod lint;

use std::path::Path;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_saphyr::{DuplicateKeyPolicy, MergeKeyPolicy, NonFiniteFloatPolicy};

pub use error::YamlError;
pub use lint::Lint;

/// Largest config file the [`Profile::Config`] parse accepts.
const CONFIG_MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;

/// Largest document the [`Profile::Data`] parse accepts.
const DATA_MAX_INPUT_BYTES: usize = 1024 * 1024 * 1024;

/// What the YAML is: it sets the parse budget and the lint passes.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    /// Operator-written configuration: one document, up to 16 MiB, 100,000
    /// nodes and 32 levels deep. Lints odd indentation and number shapes.
    Config,
    /// Operator data such as lookup tables: up to 1 GiB and 50 million nodes,
    /// alias limits kept. Lints number shapes.
    Data,
    /// YAML this process wrote, or embedded at build time: no budget, no lints.
    Trusted,
}

impl Profile {
    /// Largest input this profile parses, `None` for no limit.
    fn max_input_bytes(self) -> Option<usize> {
        match self {
            Self::Config => Some(CONFIG_MAX_INPUT_BYTES),
            Self::Data => Some(DATA_MAX_INPUT_BYTES),
            Self::Trusted => None,
        }
    }

    /// The parser budget, `None` for no limit.
    fn budget(self) -> Option<serde_saphyr::Budget> {
        match self {
            Self::Config => serde_saphyr::budget! {
                max_reader_input_bytes: Some(CONFIG_MAX_INPUT_BYTES),
                max_events: 400_000,
                max_nodes: 100_000,
                max_depth: 32,
                max_documents: 1,
                max_total_scalar_bytes: CONFIG_MAX_INPUT_BYTES,
            },
            Self::Data => serde_saphyr::budget! {
                max_reader_input_bytes: Some(DATA_MAX_INPUT_BYTES),
                max_events: 200_000_000,
                max_nodes: 50_000_000,
                max_total_scalar_bytes: DATA_MAX_INPUT_BYTES,
            },
            Self::Trusted => None,
        }
    }
}

/// Parse options for `profile`. Built per call: `serde_saphyr::Options` holds
/// `Rc` callbacks, so it is neither `Send` nor shareable.
pub(crate) fn options(profile: Profile) -> serde_saphyr::Options {
    serde_saphyr::options! {
        budget: profile.budget(),
        // Comments are never read, and emitting them caps how many may sit
        // inside one collection entry.
        emit_comments: false,
        strict_booleans: true,
        // A snippet quotes the lines around a failure, secrets included.
        with_snippet: false,
        merge_keys: MergeKeyPolicy::Merge,
        duplicate_keys: DuplicateKeyPolicy::Error,
        non_finite_float_policy: NonFiniteFloatPolicy::Reject,
        ignore_binary_tag_for_string: true,
        legacy_octal_numbers: false,
    }
}

/// A typed parse with what it skipped and what looked wrong.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct Loaded<T> {
    /// The parsed value.
    pub value: T,
    /// Dotted paths of the keys no field of `T` took, in document order.
    pub unknown_keys: Vec<String>,
    /// Warnings that did not stop the parse.
    pub lints: Vec<Lint>,
}

/// Parse `text` into `T`. `origin` names the file or source in errors.
///
/// # Errors
///
/// [`YamlError`] when `text` is over the profile's size or budget, is not
/// YAML, or does not fit `T`.
pub fn from_str<T: DeserializeOwned>(
    text: &str,
    origin: &str,
    profile: Profile,
) -> Result<T, YamlError> {
    check_size(text.len(), origin, profile)?;
    serde_saphyr::from_str_with_options(text, options(profile))
        .map_err(|e| YamlError::parse(origin, e))
}

/// Read the file at `path` and parse it into `T`. The path names the file in
/// errors.
///
/// # Errors
///
/// [`YamlError`] when the file cannot be read or is over the profile's size,
/// and every error [`from_str`] returns.
pub fn from_path<T: DeserializeOwned>(
    path: impl AsRef<Path>,
    profile: Profile,
) -> Result<T, YamlError> {
    let path = path.as_ref();
    let text = read_capped(path, profile)?;
    from_str(&text, &path.display().to_string(), profile)
}

/// Parse `text` into `T`, reporting the keys `T` did not take and the lints for
/// `profile`.
///
/// # Errors
///
/// Every error [`from_str`] returns.
pub fn from_str_reporting<T: DeserializeOwned>(
    text: &str,
    origin: &str,
    profile: Profile,
) -> Result<Loaded<T>, YamlError> {
    check_size(text.len(), origin, profile)?;
    let mut unknown_keys = Vec::new();
    let value =
        serde_saphyr::with_deserializer_from_str_with_options(text, options(profile), |de| {
            serde_ignored::deserialize(de, |path| unknown_keys.push(path.to_string()))
        })
        .map_err(|e| YamlError::parse(origin, e))?;
    Ok(Loaded {
        value,
        unknown_keys,
        lints: lint::run(text, profile),
    })
}

/// [`from_str_reporting`] over the file at `path`.
///
/// # Errors
///
/// Every error [`from_path`] returns.
pub fn from_path_reporting<T: DeserializeOwned>(
    path: impl AsRef<Path>,
    profile: Profile,
) -> Result<Loaded<T>, YamlError> {
    let path = path.as_ref();
    let text = read_capped(path, profile)?;
    from_str_reporting(&text, &path.display().to_string(), profile)
}

/// Parse `text` into an untyped value, mapping keys in document order when
/// serde_json's `preserve_order` is on.
///
/// Leading-zero and `_`-separated plain numbers become numbers here. Use
/// [`from_str_reporting`] with `serde_json::Value` to get the lints.
///
/// # Errors
///
/// Every error [`from_str`] returns.
pub fn to_value(
    text: &str,
    origin: &str,
    profile: Profile,
) -> Result<serde_json::Value, YamlError> {
    from_str(text, origin, profile)
}

/// Emit `value` as YAML.
///
/// The output is read back and compared with `value` before it is returned,
/// so text that would not read back to the same value is refused rather than
/// written. Whitespace-only lines are emptied, which changes no value.
///
/// # Errors
///
/// [`YamlError`] when `value` cannot be serialised, has no JSON form to compare
/// against, or does not read back to the same value.
pub fn to_string<T: Serialize>(value: &T) -> Result<String, YamlError> {
    const ORIGIN: &str = "emitted YAML";
    let expected = serde_json::to_value(value).map_err(|e| YamlError::emit(ORIGIN, e))?;
    let raw = serde_saphyr::to_string(value).map_err(|e| YamlError::emit(ORIGIN, e))?;
    let text = empty_blank_lines(&raw);
    let reread: serde_json::Value = from_str(&text, ORIGIN, Profile::Trusted)?;
    if values_match(&expected, &reread) {
        Ok(text)
    } else {
        Err(YamlError::round_trip(ORIGIN))
    }
}

/// Refuse input over the profile's byte limit before parsing any of it.
fn check_size(len: usize, origin: &str, profile: Profile) -> Result<(), YamlError> {
    match profile.max_input_bytes() {
        Some(max) if len > max => Err(YamlError::too_large(origin, len, max)),
        _ => Ok(()),
    }
}

/// Read a file, refusing one over the profile's byte limit before reading it.
fn read_capped(path: &Path, profile: Profile) -> Result<String, YamlError> {
    let origin = path.display().to_string();
    let len = std::fs::metadata(path)
        .map_err(|e| YamlError::io(&origin, e))?
        .len();
    check_size(usize::try_from(len).unwrap_or(usize::MAX), &origin, profile)?;
    std::fs::read_to_string(path).map_err(|e| YamlError::io(&origin, e))
}

/// Empty every line that holds only spaces or tabs, keeping the line breaks.
fn empty_blank_lines(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let (body, newline) = line
            .strip_suffix('\n')
            .map_or((line, ""), |body| (body, "\n"));
        if !body.trim_matches([' ', '\t']).is_empty() {
            out.push_str(body);
        }
        out.push_str(newline);
    }
    out
}

/// Structural equality where two floats also match within `f32` rounding, the
/// precision an `f32` field emits at.
fn values_match(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x == y
                || match (x.as_f64(), y.as_f64()) {
                    (Some(x), Some(y)) => {
                        (x - y).abs() <= f64::from(f32::EPSILON) * x.abs().max(y.abs())
                    }
                    _ => false,
                }
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| values_match(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(key, x)| y.get(key).is_some_and(|y| values_match(x, y)))
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests;
