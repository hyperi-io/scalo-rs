// Project:   scalo
// File:      src/deployment/capability.rs
// Purpose:   Capability-catalog types for reflectable config (scalo-rs#6)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Capability-catalog types.
//!
//! A derived JSON Schema (via schemars) describes the typed shape of an app's
//! `Config`, but it cannot describe runtime DATA -- service names like
//! "cloudtrail" are strings in a list, and their knobs are read ad-hoc deep in
//! the fetch code. The capability catalog fills that gap: scalo defines the
//! catalog TYPES here; each app fills the CONTENT (grounded in its own code).
//!
//! The catalog is one flat list of [`Capability`], each discriminated by
//! [`Capability::kind`] (`"source"`, `"service"`, `"transport"`, ...); nested
//! capabilities (e.g. a source's services) live under [`Capability::children`].
//!
//! This shape is a cross-language SSoT shared with scalo-py -- see
//! `docs/reflectable-config-shape.md`. Keep the two in step.
//!
//! # Example
//!
//! ```rust
//! use scalo::deployment::{Capability, FieldSpec};
//!
//! let aws = Capability::source("aws")
//!     .description("AWS audit sources (CloudTrail, GuardDuty, ...).")
//!     .maturity("stable")
//!     .field(FieldSpec::string("id").required().description("Connection id."))
//!     .field(FieldSpec::secret("secret_access_key").description("AWS secret key."))
//!     .child(
//!         Capability::service("cloudwatch_logs")
//!             .description("CloudWatch Logs for a named log group.")
//!             .field(FieldSpec::string("log_group_name").required()),
//!     );
//! assert_eq!(aws.name, "aws");
//! assert_eq!(aws.children.len(), 1);
//! ```

use serde::{Deserialize, Serialize};

/// One node in the capability catalog.
///
/// See the module docs and `docs/reflectable-config-shape.md` for the shape
/// contract shared with scalo-py.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Capability {
    /// Discriminator: `"source"`, `"service"`, `"transport"`, `"sink"`, ...
    pub kind: String,

    /// Capability name (e.g. `"aws"`, `"cloudtrail"`, `"kafka"`).
    pub name: String,

    /// Human-readable description.
    #[serde(default)]
    pub description: String,

    /// Maturity: `"alpha"`, `"beta"`, `"stable"`. Omitted when unspecified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maturity: Option<String>,

    /// Config fields for this capability (connection/service knobs). Omitted
    /// when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FieldSpec>,

    /// Nested capabilities (e.g. a source's services). Omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<Capability>,
}

impl Capability {
    /// New capability with the given `kind` and `name` (other fields empty).
    #[must_use]
    pub fn new(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            description: String::new(),
            maturity: None,
            fields: Vec::new(),
            children: Vec::new(),
        }
    }

    /// A `kind = "source"` capability.
    #[must_use]
    pub fn source(name: impl Into<String>) -> Self {
        Self::new("source", name)
    }

    /// A `kind = "service"` capability.
    #[must_use]
    pub fn service(name: impl Into<String>) -> Self {
        Self::new("service", name)
    }

    /// A `kind = "transport"` capability.
    #[must_use]
    pub fn transport(name: impl Into<String>) -> Self {
        Self::new("transport", name)
    }

    /// A `kind = "sink"` capability.
    #[must_use]
    pub fn sink(name: impl Into<String>) -> Self {
        Self::new("sink", name)
    }

    /// Set the description.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the maturity (`"alpha"` / `"beta"` / `"stable"`).
    #[must_use]
    pub fn maturity(mut self, maturity: impl Into<String>) -> Self {
        self.maturity = Some(maturity.into());
        self
    }

    /// Append one field spec.
    #[must_use]
    pub fn field(mut self, field: FieldSpec) -> Self {
        self.fields.push(field);
        self
    }

    /// Append many field specs.
    #[must_use]
    pub fn fields(mut self, fields: impl IntoIterator<Item = FieldSpec>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Append one child capability.
    #[must_use]
    pub fn child(mut self, child: Capability) -> Self {
        self.children.push(child);
        self
    }

    /// Append many child capabilities.
    #[must_use]
    pub fn children(mut self, children: impl IntoIterator<Item = Capability>) -> Self {
        self.children.extend(children);
        self
    }
}

/// A single config field within a [`Capability`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSpec {
    /// Field name.
    pub name: String,

    /// Field type. Serialised under the JSON key `type`.
    #[serde(rename = "type")]
    pub type_: FieldType,

    /// Whether the field is required.
    pub required: bool,

    /// Default value, if any. Omitted when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,

    /// Human-readable description.
    #[serde(default)]
    pub description: String,

    /// Whether the field holds a secret. When `true` the app schema also marks
    /// the field `x-scalo-secret` so the UI masks it and the engine routes it
    /// through the secrets seam. Omitted when `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub secret: bool,

    /// Allowed values for `type = enum`. Omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_values: Vec<String>,

    /// Example value. Omitted when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub example: Option<serde_json::Value>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool {
    !*b
}

impl FieldSpec {
    /// New field with the given name and type (not required, no default).
    #[must_use]
    pub fn new(name: impl Into<String>, type_: FieldType) -> Self {
        Self {
            name: name.into(),
            type_,
            required: false,
            default: None,
            description: String::new(),
            secret: false,
            enum_values: Vec::new(),
            example: None,
        }
    }

    /// A `string` field.
    #[must_use]
    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::String)
    }

    /// An `int` field.
    #[must_use]
    pub fn int(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::Int)
    }

    /// A `float` field.
    #[must_use]
    pub fn float(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::Float)
    }

    /// A `bool` field.
    #[must_use]
    pub fn bool(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::Bool)
    }

    /// A `secret` field (`type = secret`, `secret = true`).
    #[must_use]
    pub fn secret(name: impl Into<String>) -> Self {
        let mut f = Self::new(name, FieldType::Secret);
        f.secret = true;
        f
    }

    /// An `enum` field with its allowed values.
    #[must_use]
    pub fn enumeration(
        name: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut f = Self::new(name, FieldType::Enum);
        f.enum_values = values.into_iter().map(Into::into).collect();
        f
    }

    /// A `duration` field.
    #[must_use]
    pub fn duration(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::Duration)
    }

    /// A `list` field.
    #[must_use]
    pub fn list(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::List)
    }

    /// A `map` field.
    #[must_use]
    pub fn map(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::Map)
    }

    /// An `object` field.
    #[must_use]
    pub fn object(name: impl Into<String>) -> Self {
        Self::new(name, FieldType::Object)
    }

    /// Mark the field required.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Mark the field as holding a secret (without changing its `type`).
    #[must_use]
    pub fn mark_secret(mut self) -> Self {
        self.secret = true;
        self
    }

    /// Set the description.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the default value.
    #[must_use]
    pub fn default_value(mut self, value: impl Into<serde_json::Value>) -> Self {
        self.default = Some(value.into());
        self
    }

    /// Set an example value.
    #[must_use]
    pub fn example(mut self, value: impl Into<serde_json::Value>) -> Self {
        self.example = Some(value.into());
        self
    }
}

/// The type of a [`FieldSpec`]. Serialised lower-snake-case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    /// A UTF-8 string.
    #[default]
    String,
    /// A signed integer.
    Int,
    /// A floating-point number.
    Float,
    /// A boolean.
    Bool,
    /// A secret string (masked; routed via the secrets seam).
    Secret,
    /// A closed set of string values (see [`FieldSpec::enum_values`]).
    Enum,
    /// A duration (seconds unless the description says otherwise).
    Duration,
    /// A list/array of values.
    List,
    /// A string-keyed map of values.
    Map,
    /// A nested object.
    Object,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_type_serialises_snake_case() {
        assert_eq!(
            serde_json::to_value(FieldType::Secret).unwrap(),
            serde_json::json!("secret")
        );
        assert_eq!(
            serde_json::to_value(FieldType::Duration).unwrap(),
            serde_json::json!("duration")
        );
    }

    #[test]
    fn secret_field_flags_secret_and_type() {
        let f = FieldSpec::secret("password");
        assert_eq!(f.type_, FieldType::Secret);
        assert!(f.secret);
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], "secret");
        assert_eq!(v["secret"], true);
    }

    #[test]
    fn empty_and_default_fields_are_omitted() {
        // A plain, non-secret, non-required string field with no default:
        // name/type/required/description are always present (core catalog
        // fields); secret/default/example/enum_values are omitted for a clean,
        // deterministic catalog.
        let f = FieldSpec::string("region");
        let v = serde_json::to_value(&f).unwrap();
        let obj = v.as_object().unwrap();
        assert_eq!(obj.len(), 4, "expected name/type/required/description: {v}");
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("type"));
        assert!(obj.contains_key("required"));
        assert!(obj.contains_key("description"));
        assert!(!obj.contains_key("secret"));
        assert!(!obj.contains_key("default"));
        assert!(!obj.contains_key("enum_values"));
        assert!(!obj.contains_key("example"));
    }

    #[test]
    fn capability_builder_nests_children() {
        let cap = Capability::source("aws")
            .description("AWS sources.")
            .maturity("stable")
            .field(FieldSpec::string("id").required())
            .child(Capability::service("cloudtrail").maturity("stable"));
        assert_eq!(cap.kind, "source");
        assert_eq!(cap.fields.len(), 1);
        assert_eq!(cap.children.len(), 1);
        assert_eq!(cap.children[0].kind, "service");

        let v = serde_json::to_value(&cap).unwrap();
        assert_eq!(v["kind"], "source");
        assert_eq!(v["maturity"], "stable");
        assert_eq!(v["fields"][0]["name"], "id");
        assert_eq!(v["fields"][0]["required"], true);
        assert_eq!(v["children"][0]["name"], "cloudtrail");
    }

    #[test]
    fn empty_capability_omits_optional_collections() {
        let cap = Capability::service("plain");
        let v = serde_json::to_value(&cap).unwrap();
        let obj = v.as_object().unwrap();
        assert!(!obj.contains_key("maturity"));
        assert!(!obj.contains_key("fields"));
        assert!(!obj.contains_key("children"));
    }

    #[test]
    fn catalog_round_trips_through_json() {
        let cap = Capability::source("okta")
            .field(FieldSpec::secret("token").description("SSWS token."))
            .field(FieldSpec::enumeration("include", ["all", "web", "git"]));
        let json = serde_json::to_string(&cap).unwrap();
        let back: Capability = serde_json::from_str(&json).unwrap();
        assert_eq!(cap, back);
    }
}
