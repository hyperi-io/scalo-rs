// Project:   scalo
// File:      src/rules/config.rs
// Purpose:   The rule set as an operator writes it
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Serde shapes of a rule set. [`RuleSet::compile`](super::RuleSet::compile)
//! checks them and builds the evaluator.
//!
//! ```yaml
//! name: ingest
//! targets: [_source, _org_id]
//! lookups:
//!   aliases: { logs_syslog: syslog }
//! rules:
//!   - id: category
//!     set: { _source: { lookup: aliases, from: tags.event.category } }
//!   - id: web-hosts
//!     match: { field: host, op: starts_with, value: web- }
//!     set: { _source: { value: web } }
//!   - id: org-tags
//!     match: { field: tags.event.org_id, op: exists }
//!     set: { _org_id: { from: tags.event.org_id } }
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The metric label and log name a rule set takes when it names none.
pub const DEFAULT_RULE_SET_NAME: &str = "rules";

fn default_name() -> String {
    DEFAULT_RULE_SET_NAME.to_string()
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

/// An ordered rule set: per target field, the first row that matches sets it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RuleSetConfig {
    /// The `rule_set` metric label. Defaults to `rules`.
    #[serde(default = "default_name")]
    pub name: String,
    /// The fields the set assigns, in evaluation order. A row for a later
    /// target reads an earlier target's new value.
    #[serde(default)]
    pub targets: Vec<String>,
    /// Bounded tables a `lookup` action maps a value through, by name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub lookups: BTreeMap<String, BTreeMap<String, String>>,
    /// The rows, in declared order.
    #[serde(default)]
    pub rules: Vec<RuleConfig>,
}

impl Default for RuleSetConfig {
    fn default() -> Self {
        Self {
            name: default_name(),
            targets: Vec::new(),
            lookups: BTreeMap::new(),
            rules: Vec::new(),
        }
    }
}

impl RuleSetConfig {
    /// An empty set named `name` that assigns `targets` in that order.
    #[must_use]
    pub fn new<I, S>(name: impl Into<String>, targets: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            targets: targets.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Append `rule` after the rows already declared.
    #[must_use]
    pub fn rule(mut self, rule: RuleConfig) -> Self {
        self.rules.push(rule);
        self
    }

    /// Add the lookup table `name`.
    #[must_use]
    pub fn lookup<I, K, V>(mut self, name: impl Into<String>, entries: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.lookups.insert(
            name.into(),
            entries
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }
}

/// One row: when it matches, it sets one target field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RuleConfig {
    /// Stable id: the `rule` metric label, and how an editor names the row.
    pub id: String,
    /// A native test. With neither `match` nor `when` the row always matches.
    #[serde(default, rename = "match", skip_serializing_if = "Option::is_none")]
    pub matcher: Option<MatchConfig>,
    /// A CEL Fast condition, in place of `match`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// Exactly one entry: the target field and how its value is found.
    pub set: BTreeMap<String, SetAction>,
}

impl RuleConfig {
    /// A row `id` that always matches and sets `target` by `action`.
    #[must_use]
    pub fn new(id: impl Into<String>, target: impl Into<String>, action: SetAction) -> Self {
        Self {
            id: id.into(),
            matcher: None,
            when: None,
            set: BTreeMap::from([(target.into(), action)]),
        }
    }

    /// Match only when `matcher` holds.
    #[must_use]
    pub fn matching(mut self, matcher: MatchConfig) -> Self {
        self.matcher = Some(matcher);
        self
    }

    /// Match only when the CEL Fast `condition` is true.
    #[must_use]
    pub fn when(mut self, condition: impl Into<String>) -> Self {
        self.when = Some(condition.into());
        self
    }
}

/// A native test against one field, or `all` of several.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct MatchConfig {
    /// Dotted path of the field tested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// The test.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<MatchOp>,
    /// Operand of `equals`, `starts_with`, `ends_with`, `contains` and `regex`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Leading text of `prefix_and_suffix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Trailing text of `prefix_and_suffix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
    /// Fewest characters `length` accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<usize>,
    /// Most characters `length` accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<usize>,
    /// Invert the result. A negated test matches a record without the field.
    #[serde(default, skip_serializing_if = "is_false")]
    pub negate: bool,
    /// Every one of these, in place of `field` and `op`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub all: Vec<MatchConfig>,
}

impl MatchConfig {
    fn leaf(field: impl Into<String>, op: MatchOp, value: Option<String>) -> Self {
        Self {
            field: Some(field.into()),
            op: Some(op),
            value,
            ..Self::default()
        }
    }

    /// The field's text is `value`.
    #[must_use]
    pub fn equals(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(field, MatchOp::Equals, Some(value.into()))
    }

    /// The field's text starts with `value`.
    #[must_use]
    pub fn starts_with(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(field, MatchOp::StartsWith, Some(value.into()))
    }

    /// The field's text ends with `value`.
    #[must_use]
    pub fn ends_with(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(field, MatchOp::EndsWith, Some(value.into()))
    }

    /// The field's text contains `value`.
    #[must_use]
    pub fn contains(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(field, MatchOp::Contains, Some(value.into()))
    }

    /// The field's text starts with `prefix` and ends with `suffix`.
    #[must_use]
    pub fn prefix_and_suffix(
        field: impl Into<String>,
        prefix: impl Into<String>,
        suffix: impl Into<String>,
    ) -> Self {
        Self {
            prefix: Some(prefix.into()),
            suffix: Some(suffix.into()),
            ..Self::leaf(field, MatchOp::PrefixAndSuffix, None)
        }
    }

    /// The field's text is `min` to `max` characters long, either bound open.
    #[must_use]
    pub fn length(field: impl Into<String>, min: Option<usize>, max: Option<usize>) -> Self {
        Self {
            min,
            max,
            ..Self::leaf(field, MatchOp::Length, None)
        }
    }

    /// The field is present, not null and not empty text.
    #[must_use]
    pub fn exists(field: impl Into<String>) -> Self {
        Self::leaf(field, MatchOp::Exists, None)
    }

    /// The field's text matches the regex `pattern`, where the profile allows regex.
    #[must_use]
    pub fn regex(field: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::leaf(field, MatchOp::Regex, Some(pattern.into()))
    }

    /// Every one of `parts` holds.
    #[must_use]
    pub fn all(parts: Vec<MatchConfig>) -> Self {
        Self {
            all: parts,
            ..Self::default()
        }
    }

    /// Invert the test.
    #[must_use]
    pub fn negate(mut self) -> Self {
        self.negate = true;
        self
    }
}

/// A native test. Every test but `exists` reads the field's text: a string as
/// written, a number or boolean as its JSON text. Null, an object or an array
/// fails every test but a negated one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MatchOp {
    /// Text equals `value`.
    Equals,
    /// Text starts with `value`.
    StartsWith,
    /// Text ends with `value`.
    EndsWith,
    /// Text contains `value`.
    Contains,
    /// Text starts with `prefix` and ends with `suffix`.
    PrefixAndSuffix,
    /// Text is `min` to `max` characters long.
    Length,
    /// Present, not null, and not empty text.
    Exists,
    /// Text matches the regex `value`. Refused unless the CEL Fast profile allows regex.
    Regex,
}

impl MatchOp {
    /// The name the config uses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equals => "equals",
            Self::StartsWith => "starts_with",
            Self::EndsWith => "ends_with",
            Self::Contains => "contains",
            Self::PrefixAndSuffix => "prefix_and_suffix",
            Self::Length => "length",
            Self::Exists => "exists",
            Self::Regex => "regex",
        }
    }
}

/// How a matching row finds the value it sets.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SetAction {
    /// This literal text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// The text of this field. A record without it does not match the row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// The `from` text mapped through this lookup table. A value the table
    /// does not hold does not match the row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookup: Option<String>,
}

impl SetAction {
    /// Set the literal `value`.
    #[must_use]
    pub fn value(value: impl Into<String>) -> Self {
        Self {
            value: Some(value.into()),
            ..Self::default()
        }
    }

    /// Copy the text of `field`.
    #[must_use]
    pub fn from(field: impl Into<String>) -> Self {
        Self {
            from: Some(field.into()),
            ..Self::default()
        }
    }

    /// Map the text of `field` through the lookup table `table`.
    #[must_use]
    pub fn lookup(table: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            lookup: Some(table.into()),
            from: Some(field.into()),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "yaml")]
    #[test]
    fn the_documented_shape_reads() {
        let yaml = r"
name: ingest
targets: [_source, _org_id]
lookups:
  aliases: { logs_syslog: syslog }
rules:
  - id: category
    set: { _source: { lookup: aliases, from: tags.event.category } }
  - id: web-hosts
    match: { field: host, op: starts_with, value: web- }
    set: { _source: { value: web } }
  - id: not-internal
    match: { all: [ { field: host, op: contains, value: internal, negate: true } ] }
    set: { _org_id: { from: org_id } }
";
        let parsed: RuleSetConfig =
            crate::yaml::from_str(yaml, "rules.yaml", crate::yaml::Profile::Config).unwrap();
        let built = RuleSetConfig::new("ingest", ["_source", "_org_id"])
            .lookup("aliases", [("logs_syslog", "syslog")])
            .rule(RuleConfig::new(
                "category",
                "_source",
                SetAction::lookup("aliases", "tags.event.category"),
            ))
            .rule(
                RuleConfig::new("web-hosts", "_source", SetAction::value("web"))
                    .matching(MatchConfig::starts_with("host", "web-")),
            )
            .rule(
                RuleConfig::new("not-internal", "_org_id", SetAction::from("org_id")).matching(
                    MatchConfig::all(vec![MatchConfig::contains("host", "internal").negate()]),
                ),
            );
        assert_eq!(parsed, built);
    }

    #[test]
    fn a_misspelt_key_is_refused() {
        let json = r#"{"rules": [{"id": "a", "set": {"x": {"vaule": "y"}}}]}"#;
        let parsed: Result<RuleSetConfig, _> = serde_json::from_str(json);
        assert!(parsed.is_err());
    }

    #[test]
    fn an_unnamed_set_takes_the_default_name() {
        let parsed: RuleSetConfig = serde_json::from_str(r#"{"targets": ["a"]}"#).unwrap();
        assert_eq!(parsed.name, DEFAULT_RULE_SET_NAME);
    }

    #[test]
    fn every_op_name_round_trips() {
        for op in [
            MatchOp::Equals,
            MatchOp::StartsWith,
            MatchOp::EndsWith,
            MatchOp::Contains,
            MatchOp::PrefixAndSuffix,
            MatchOp::Length,
            MatchOp::Exists,
            MatchOp::Regex,
        ] {
            let text = serde_json::to_string(&op).unwrap();
            assert_eq!(text, format!("\"{}\"", op.as_str()));
            let back: MatchOp = serde_json::from_str(&text).unwrap();
            assert_eq!(back, op);
        }
    }
}
