// Project:   scalo
// File:      src/rules/compile.rs
// Purpose:   Check a rule set config and build the evaluator from it
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One pass over the config collects every finding, so an editor shows them
//! all at once, then builds the [`RuleSet`] when none is an error.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use super::config::{MatchConfig, MatchOp, RuleConfig, RuleSetConfig, SetAction};
use super::error::{RuleDiagnostic, RulesError};
use super::field::FieldPath;
use super::usage::Usage;
use super::{CompileOptions, MAX_TARGETS};
use crate::strmatch::StrMatcher;

/// Longest rule id, rule set name or lookup table name.
const MAX_ID_LEN: usize = 128;

/// Index into [`RuleSet::fields`].
pub(crate) type FieldId = usize;

/// A compiled, immutable rule set. Share it behind an `Arc`, and evaluate
/// through `&self`.
pub struct RuleSet {
    pub(crate) name: Box<str>,
    pub(crate) targets: Box<[Target]>,
    /// Ordered by target, then as declared.
    pub(crate) rules: Box<[CompiledRule]>,
    pub(crate) fields: Box<[FieldSlot]>,
    pub(crate) tables: Box<[HashMap<String, String>]>,
    /// Built once per set: `Env::stdlib()` builds every function declaration.
    pub(crate) cel_env: Option<Arc<cel::Env>>,
    pub(crate) warnings: Vec<RuleDiagnostic>,
    pub(crate) usage: Usage,
}

impl std::fmt::Debug for RuleSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuleSet")
            .field("name", &self.name)
            .field(
                "targets",
                &self
                    .targets
                    .iter()
                    .map(|t| t.field.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("rules", &self.rules.len())
            .finish_non_exhaustive()
    }
}

pub(crate) struct Target {
    pub(crate) field: FieldPath,
    pub(crate) rules: Range<usize>,
}

pub(crate) struct FieldSlot {
    pub(crate) path: FieldPath,
    /// The target this path names, whose new value a later target reads.
    pub(crate) target: Option<usize>,
}

pub(crate) struct CompiledRule {
    pub(crate) id: Box<str>,
    pub(crate) target: usize,
    pub(crate) condition: Condition,
    pub(crate) action: Action,
}

pub(crate) enum Condition {
    Always,
    Native(Matcher),
    Cel(cel::Program),
}

pub(crate) enum Matcher {
    Leaf {
        field: FieldId,
        test: Test,
        negate: bool,
    },
    All {
        parts: Box<[Matcher]>,
        negate: bool,
    },
}

pub(crate) enum Test {
    Equals(Box<str>),
    StartsWith(Box<str>),
    EndsWith(Box<str>),
    Contains(Box<memchr::memmem::Finder<'static>>),
    PrefixAndSuffix { prefix: Box<str>, suffix: Box<str> },
    Length { min: usize, max: usize },
    Exists,
    Regex(Box<StrMatcher>),
}

pub(crate) enum Action {
    Value(Box<str>),
    From(FieldId),
    Lookup { table: usize, from: FieldId },
}

impl RuleSet {
    /// Compile `config` under the CEL Fast profile the config cascade sets in
    /// `expression`, or the default profile before the cascade loads.
    ///
    /// # Errors
    ///
    /// [`RulesError::Invalid`] with every finding when any is an error.
    pub fn compile(config: &RuleSetConfig) -> Result<Self, RulesError> {
        Self::compile_with(config, &CompileOptions::from_cascade())
    }

    /// Compile `config` with explicit options.
    ///
    /// Warnings do not stop the set compiling. [`warnings`](Self::warnings)
    /// returns them.
    ///
    /// # Errors
    ///
    /// [`RulesError::Invalid`] with every finding when any is an error.
    pub fn compile_with(
        config: &RuleSetConfig,
        options: &CompileOptions,
    ) -> Result<Self, RulesError> {
        let mut builder = Builder::new(options);
        let built = builder.build(config);
        match built {
            Some(set) if !builder.has_errors() => Ok(set),
            _ => Err(RulesError::Invalid(builder.found)),
        }
    }

    /// Every finding in `config`, without building a set. Empty when the set
    /// compiles clean. Warnings are looked for once there is no error.
    #[must_use]
    pub fn check(config: &RuleSetConfig, options: &CompileOptions) -> Vec<RuleDiagnostic> {
        let mut builder = Builder::new(options);
        let _ = builder.build(config);
        builder.found
    }

    /// Rows the set compiled with that never fire as written.
    #[must_use]
    pub fn warnings(&self) -> &[RuleDiagnostic] {
        &self.warnings
    }

    /// The `rule_set` metric label.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// True when the set has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

/// Why `id` is not a rule id, or `None` when it is one.
fn id_fault(id: &str) -> Option<&'static str> {
    if id.is_empty() || id.len() > MAX_ID_LEN {
        return Some("is not 1 to 128 characters long");
    }
    if !id.starts_with(|c: char| c.is_ascii_alphanumeric()) {
        return Some("does not start with a letter or digit");
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._:/@-".contains(&b))
    {
        return Some("holds a character other than a letter, a digit or one of . _ : / @ -");
    }
    None
}

/// Collects findings while it compiles, so one pass reports them all.
struct Builder<'o> {
    options: &'o CompileOptions,
    found: Vec<RuleDiagnostic>,
    fields: Vec<FieldSlot>,
    field_index: HashMap<String, FieldId>,
    target_index: HashMap<String, usize>,
    table_index: BTreeMap<String, usize>,
    needs_cel: bool,
}

impl<'o> Builder<'o> {
    fn new(options: &'o CompileOptions) -> Self {
        Self {
            options,
            found: Vec::new(),
            fields: Vec::new(),
            field_index: HashMap::new(),
            target_index: HashMap::new(),
            table_index: BTreeMap::new(),
            needs_cel: false,
        }
    }

    fn has_errors(&self) -> bool {
        self.found.iter().any(RuleDiagnostic::is_error)
    }

    fn error(&mut self, location: impl Into<String>, message: impl Into<String>) {
        self.found.push(RuleDiagnostic::error(location, message));
    }

    fn rule_error(&mut self, id: &str, location: impl Into<String>, message: impl Into<String>) {
        self.found
            .push(RuleDiagnostic::error(location, message).for_rule(id));
    }

    /// The interned id of `text`, or `None` (with an error) when it is not a path.
    fn field(&mut self, id: &str, location: &str, text: &str) -> Option<FieldId> {
        if let Some(&known) = self.field_index.get(text) {
            return Some(known);
        }
        let Some(path) = FieldPath::parse(text) else {
            self.rule_error(
                id,
                location,
                format!("{text:?} is not a dotted field path with no empty segment"),
            );
            return None;
        };
        let index = self.fields.len();
        self.fields.push(FieldSlot {
            target: self.target_index.get(text).copied(),
            path,
        });
        self.field_index.insert(text.to_string(), index);
        Some(index)
    }

    fn build(&mut self, config: &RuleSetConfig) -> Option<RuleSet> {
        if let Some(fault) = id_fault(&config.name) {
            self.error("name", format!("{:?} {fault}", config.name));
        }
        let targets = self.targets(config);
        let tables = self.tables(config);
        let mut compiled: Vec<CompiledRule> = Vec::with_capacity(config.rules.len());
        let mut ids: HashSet<&str> = HashSet::new();
        for (index, rule) in config.rules.iter().enumerate() {
            if !ids.insert(rule.id.as_str()) {
                self.rule_error(
                    &rule.id,
                    format!("rules[{index}].id"),
                    format!(
                        "{:?} is the id of an earlier row, and ids are unique",
                        rule.id
                    ),
                );
            }
            if let Some(rule) = self.rule(index, rule) {
                compiled.push(rule);
            }
        }
        if self.has_errors() {
            return None;
        }
        self.shadow_warnings(config);

        // Group by target, keeping declared order within each.
        compiled.sort_by_key(|rule| rule.target);
        let mut ranges = vec![0..0; targets.len()];
        let mut start = 0;
        for (t, range) in ranges.iter_mut().enumerate() {
            let len = compiled[start..]
                .iter()
                .take_while(|r| r.target == t)
                .count();
            *range = start..start + len;
            start += len;
        }
        let targets: Box<[Target]> = targets
            .into_iter()
            .zip(ranges)
            .map(|(field, rules)| Target { field, rules })
            .collect();
        let ids: Vec<&str> = compiled.iter().map(|r| &*r.id).collect();
        let usage = Usage::new(&config.name, &ids);
        Some(RuleSet {
            name: config.name.as_str().into(),
            targets,
            rules: compiled.into(),
            fields: std::mem::take(&mut self.fields).into(),
            tables: tables.into(),
            cel_env: self.needs_cel.then(|| Arc::new(cel::Env::stdlib())),
            warnings: self
                .found
                .iter()
                .filter(|d| !d.is_error())
                .cloned()
                .collect(),
            usage,
        })
    }

    fn targets(&mut self, config: &RuleSetConfig) -> Vec<FieldPath> {
        if config.targets.is_empty() && !config.rules.is_empty() {
            self.error(
                "targets",
                "names no field, so no row has a target. List the fields the set assigns, in evaluation order",
            );
        }
        if config.targets.len() > MAX_TARGETS {
            self.error(
                "targets",
                format!(
                    "lists {} fields, and a set assigns at most {MAX_TARGETS}",
                    config.targets.len()
                ),
            );
        }
        let mut paths = Vec::with_capacity(config.targets.len());
        for (index, text) in config.targets.iter().enumerate() {
            match FieldPath::parse(text) {
                None => self.error(
                    format!("targets[{index}]"),
                    format!("{text:?} is not a dotted field path with no empty segment"),
                ),
                Some(_) if self.target_index.contains_key(text) => self.error(
                    format!("targets[{index}]"),
                    format!("{text:?} is listed twice"),
                ),
                Some(path) => {
                    self.target_index.insert(text.clone(), paths.len());
                    paths.push(path);
                }
            }
        }
        paths
    }

    fn tables(&mut self, config: &RuleSetConfig) -> Vec<HashMap<String, String>> {
        let mut tables = Vec::with_capacity(config.lookups.len());
        for (name, entries) in &config.lookups {
            let location = format!("lookups.{name}");
            if let Some(fault) = id_fault(name) {
                self.error(&location, format!("the table name {name:?} {fault}"));
            }
            if entries.len() > self.options.max_lookup_entries {
                self.error(
                    &location,
                    format!(
                        "holds {} entries, over the bound of {}",
                        entries.len(),
                        self.options.max_lookup_entries
                    ),
                );
            }
            if entries.contains_key("") {
                self.error(&location, "has an empty key, which no value ever reaches");
            }
            if let Some((key, _)) = entries.iter().find(|(_, v)| v.is_empty()) {
                self.error(
                    &location,
                    format!("maps {key:?} to empty text, which reads as no value"),
                );
            }
            self.table_index.insert(name.clone(), tables.len());
            tables.push(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
        }
        tables
    }

    fn rule(&mut self, index: usize, rule: &RuleConfig) -> Option<CompiledRule> {
        let id = rule.id.as_str();
        let at = format!("rules[{index}]");
        if let Some(fault) = id_fault(id) {
            self.rule_error(id, format!("{at}.id"), format!("{id:?} {fault}"));
        }
        let mut entries = rule.set.iter();
        let (Some((target_name, action)), None) = (entries.next(), entries.next()) else {
            self.rule_error(
                id,
                format!("{at}.set"),
                format!(
                    "names {} fields, and a row sets exactly one",
                    rule.set.len()
                ),
            );
            return None;
        };
        let target = self.target_index.get(target_name).copied();
        if target.is_none() {
            self.rule_error(
                id,
                format!("{at}.set.{target_name}"),
                format!("{target_name:?} is not one of the set's targets"),
            );
        }
        let action = self.action(id, &format!("{at}.set.{target_name}"), action);
        let condition = match (&rule.matcher, &rule.when) {
            (Some(_), Some(_)) => {
                self.rule_error(id, at, "has both `match` and `when`, and a row takes one");
                None
            }
            (Some(matcher), None) => self
                .matcher(id, &format!("{at}.match"), matcher)
                .map(Condition::Native),
            (None, Some(when)) => self.when(id, &format!("{at}.when"), when),
            (None, None) => Some(Condition::Always),
        };
        Some(CompiledRule {
            id: id.into(),
            target: target?,
            condition: condition?,
            action: action?,
        })
    }

    fn action(&mut self, id: &str, at: &str, action: &SetAction) -> Option<Action> {
        match (&action.value, &action.from, &action.lookup) {
            (Some(value), None, None) => {
                if value.is_empty() {
                    self.rule_error(
                        id,
                        format!("{at}.value"),
                        "is empty text, which reads as no value",
                    );
                    return None;
                }
                Some(Action::Value(value.as_str().into()))
            }
            (None, Some(from), None) => self
                .field(id, &format!("{at}.from"), from)
                .map(Action::From),
            (None, Some(from), Some(table)) => {
                let field = self.field(id, &format!("{at}.from"), from);
                let table_index = self.table_index.get(table).copied();
                if table_index.is_none() {
                    self.rule_error(
                        id,
                        format!("{at}.lookup"),
                        format!("{table:?} names no table in `lookups`"),
                    );
                }
                Some(Action::Lookup {
                    table: table_index?,
                    from: field?,
                })
            }
            (None, None, Some(_)) => {
                self.rule_error(
                    id,
                    at,
                    "`lookup` needs `from`, the field whose text it maps",
                );
                None
            }
            (None, None, None) => {
                self.rule_error(
                    id,
                    at,
                    "sets nothing: give `value`, `from`, or `lookup` with `from`",
                );
                None
            }
            (Some(_), _, _) => {
                self.rule_error(id, at, "has `value` beside `from` or `lookup`, so give one");
                None
            }
        }
    }

    fn when(&mut self, id: &str, at: &str, when: &str) -> Option<Condition> {
        match crate::expression::evaluator::compile_diagnosed(when, &self.options.profile) {
            Ok(program) => {
                self.needs_cel = true;
                Some(Condition::Cel(program))
            }
            Err(found) => {
                for d in found {
                    self.found
                        .push(RuleDiagnostic::error(at, d.message).for_rule(id).at(d.span));
                }
                None
            }
        }
    }

    fn matcher(&mut self, id: &str, at: &str, m: &MatchConfig) -> Option<Matcher> {
        if !m.all.is_empty() {
            let own = [
                ("field", m.field.is_some()),
                ("op", m.op.is_some()),
                ("value", m.value.is_some()),
                ("prefix", m.prefix.is_some()),
                ("suffix", m.suffix.is_some()),
                ("min", m.min.is_some()),
                ("max", m.max.is_some()),
            ];
            for (key, set) in own {
                if set {
                    self.rule_error(id, format!("{at}.{key}"), "does not apply beside `all`");
                }
            }
            let parts: Vec<Option<Matcher>> = m
                .all
                .iter()
                .enumerate()
                .map(|(i, part)| self.matcher(id, &format!("{at}.all[{i}]"), part))
                .collect();
            let parts: Option<Vec<Matcher>> = parts.into_iter().collect();
            return Some(Matcher::All {
                parts: parts?.into(),
                negate: m.negate,
            });
        }
        let (Some(field_text), Some(op)) = (&m.field, m.op) else {
            self.rule_error(id, at, "needs `field` and `op`, or `all`");
            return None;
        };
        let field = self.field(id, &format!("{at}.field"), field_text);
        let test = self.test(id, at, op, m);
        Some(Matcher::Leaf {
            field: field?,
            test: test?,
            negate: m.negate,
        })
    }

    fn test(&mut self, id: &str, at: &str, op: MatchOp, m: &MatchConfig) -> Option<Test> {
        let name = op.as_str();
        let wants: &[&str] = match op {
            MatchOp::Equals
            | MatchOp::StartsWith
            | MatchOp::EndsWith
            | MatchOp::Contains
            | MatchOp::Regex => &["value"],
            MatchOp::PrefixAndSuffix => &["prefix", "suffix"],
            MatchOp::Length => &["min", "max"],
            MatchOp::Exists => &[],
        };
        let given = [
            ("value", m.value.is_some()),
            ("prefix", m.prefix.is_some()),
            ("suffix", m.suffix.is_some()),
            ("min", m.min.is_some()),
            ("max", m.max.is_some()),
        ];
        let mut stray = false;
        for (key, set) in given {
            if set && !wants.contains(&key) {
                self.rule_error(
                    id,
                    format!("{at}.{key}"),
                    format!("does not apply to `{name}`"),
                );
                stray = true;
            }
        }
        if wants == ["value"] && m.value.is_none() {
            self.rule_error(id, format!("{at}.value"), format!("`{name}` needs `value`"));
            return None;
        }
        let value = || m.value.as_deref().map(Box::<str>::from);
        let test = match op {
            MatchOp::Equals => value().map(Test::Equals),
            MatchOp::StartsWith => value().map(Test::StartsWith),
            MatchOp::EndsWith => value().map(Test::EndsWith),
            MatchOp::Contains => m.value.as_deref().map(|needle| {
                Test::Contains(Box::new(
                    memchr::memmem::Finder::new(needle.as_bytes()).into_owned(),
                ))
            }),
            MatchOp::PrefixAndSuffix => {
                if let (Some(prefix), Some(suffix)) = (&m.prefix, &m.suffix) {
                    Some(Test::PrefixAndSuffix {
                        prefix: prefix.as_str().into(),
                        suffix: suffix.as_str().into(),
                    })
                } else {
                    self.rule_error(
                        id,
                        at,
                        "`prefix_and_suffix` needs both `prefix` and `suffix`",
                    );
                    None
                }
            }
            MatchOp::Length => self.length(id, at, m.min, m.max),
            MatchOp::Exists => Some(Test::Exists),
            MatchOp::Regex => self.regex(id, at, m.value.as_deref()),
        };
        if stray { None } else { test }
    }

    fn length(
        &mut self,
        id: &str,
        at: &str,
        min: Option<usize>,
        max: Option<usize>,
    ) -> Option<Test> {
        match (min, max) {
            (None, None) => {
                self.rule_error(id, at, "`length` needs `min`, `max` or both");
                None
            }
            (Some(min), Some(max)) if min > max => {
                self.rule_error(id, format!("{at}.min"), format!("{min} is over max {max}"));
                None
            }
            (min, max) => Some(Test::Length {
                min: min.unwrap_or(0),
                max: max.unwrap_or(usize::MAX),
            }),
        }
    }

    fn regex(&mut self, id: &str, at: &str, pattern: Option<&str>) -> Option<Test> {
        let pattern = pattern?;
        if !self.options.profile.allow_regex {
            self.rule_error(
                id,
                format!("{at}.op"),
                "`regex` is off in the CEL Fast profile, as regex has unbounded cost per \
                 record. Use starts_with, ends_with or contains, or set allow_regex: true in \
                 expression config",
            );
            return None;
        }
        match StrMatcher::new(pattern) {
            Ok(matcher) => Some(Test::Regex(Box::new(matcher))),
            Err(e) => {
                self.rule_error(id, format!("{at}.value"), e.to_string());
                None
            }
        }
    }

    /// Warn on each row an earlier row of the same target always fires before.
    fn shadow_warnings(&mut self, config: &RuleSetConfig) {
        let mut by_target: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (index, rule) in config.rules.iter().enumerate() {
            if let Some(target) = rule.set.keys().next() {
                by_target.entry(target.as_str()).or_default().push(index);
            }
        }
        for rows in by_target.values() {
            for (k, &j) in rows.iter().enumerate() {
                let later = &config.rules[j];
                let shadow = rows[..k]
                    .iter()
                    .map(|&i| &config.rules[i])
                    .find(|earlier| shadows(earlier, later));
                if let Some(earlier) = shadow {
                    self.found.push(
                        RuleDiagnostic::warning(
                            format!("rules[{j}]"),
                            format!(
                                "never fires: row {:?} sets a literal value and matches every \
                                 record this one does, so it always fires first",
                                earlier.id
                            ),
                        )
                        .for_rule(&later.id),
                    );
                }
            }
        }
    }
}

/// True when `earlier` fires on every record `later` matches. Only a literal
/// `value` always resolves, as `from` and `lookup` can miss and fall through.
fn shadows(earlier: &RuleConfig, later: &RuleConfig) -> bool {
    let literal = earlier
        .set
        .values()
        .next()
        .is_some_and(|a| a.value.is_some());
    if !literal {
        return false;
    }
    match (&earlier.matcher, &earlier.when) {
        (None, None) => true,
        (None, Some(when)) => {
            later.matcher.is_none() && later.when.as_deref().map(str::trim) == Some(when.trim())
        }
        (Some(matcher), None) => {
            later.when.is_none()
                && later.matcher.as_ref().is_some_and(|l| {
                    l == matcher || exists_field(matcher).is_some_and(|field| needs_text(l, field))
                })
        }
        (Some(_), Some(_)) => false,
    }
}

/// The field of a plain, un-negated `exists` test.
fn exists_field(m: &MatchConfig) -> Option<&str> {
    (m.all.is_empty() && !m.negate && m.op == Some(MatchOp::Exists))
        .then_some(m.field.as_deref())
        .flatten()
}

/// True when `m` can only hold where `field` holds non-empty text or a value.
fn needs_text(m: &MatchConfig, field: &str) -> bool {
    if m.negate {
        return false;
    }
    if !m.all.is_empty() {
        return m.all.iter().any(|part| needs_text(part, field));
    }
    if m.field.as_deref() != Some(field) {
        return false;
    }
    let non_empty = |text: &Option<String>| text.as_deref().is_some_and(|t| !t.is_empty());
    match m.op {
        Some(MatchOp::Exists) => true,
        Some(MatchOp::Equals | MatchOp::StartsWith | MatchOp::EndsWith | MatchOp::Contains) => {
            non_empty(&m.value)
        }
        Some(MatchOp::PrefixAndSuffix) => non_empty(&m.prefix) || non_empty(&m.suffix),
        Some(MatchOp::Length) => m.min.is_some_and(|min| min >= 1),
        Some(MatchOp::Regex) | None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::ProfileConfig;
    use crate::rules::error::Severity;

    fn options() -> CompileOptions {
        CompileOptions::default()
    }

    fn errors(config: &RuleSetConfig) -> Vec<RuleDiagnostic> {
        RuleSet::check(config, &options())
            .into_iter()
            .filter(RuleDiagnostic::is_error)
            .collect()
    }

    fn locations(found: &[RuleDiagnostic]) -> Vec<&str> {
        found.iter().map(|d| d.location.as_str()).collect()
    }

    fn base() -> RuleSetConfig {
        RuleSetConfig::new("test", ["_source", "_org_id"])
    }

    #[test]
    fn a_duplicate_rule_id_is_refused() {
        let config = base()
            .rule(RuleConfig::new("a", "_source", SetAction::value("x")))
            .rule(RuleConfig::new("a", "_org_id", SetAction::value("y")));
        let found = errors(&config);
        assert_eq!(locations(&found), ["rules[1].id"]);
        assert_eq!(found[0].rule.as_deref(), Some("a"));
        assert!(RuleSet::compile_with(&config, &options()).is_err());
    }

    #[test]
    fn every_error_in_a_set_is_reported_in_one_pass() {
        let config = base()
            .rule(RuleConfig::new("bad id!", "_source", SetAction::value("x")))
            .rule(RuleConfig::new("b", "nowhere", SetAction::value("y")))
            .rule(
                RuleConfig::new("c", "_source", SetAction::lookup("missing", "f"))
                    .matching(MatchConfig::equals("..", "x")),
            );
        let found = errors(&config);
        assert_eq!(
            locations(&found),
            [
                "rules[0].id",
                "rules[1].set.nowhere",
                "rules[2].set._source.lookup",
                "rules[2].match.field",
            ]
        );
    }

    #[test]
    fn a_target_list_must_name_each_field_once() {
        let config = RuleSetConfig::new("t", ["a", "a", "b."]).rule(RuleConfig::new(
            "r",
            "a",
            SetAction::value("x"),
        ));
        assert_eq!(locations(&errors(&config)), ["targets[1]", "targets[2]"]);
        let none = RuleSetConfig::new("t", Vec::<String>::new()).rule(RuleConfig::new(
            "r",
            "a",
            SetAction::value("x"),
        ));
        assert!(locations(&errors(&none)).contains(&"targets"));
    }

    #[test]
    fn an_empty_set_with_no_targets_compiles() {
        let empty = RuleSetConfig::new("t", Vec::<String>::new());
        let set = RuleSet::compile_with(&empty, &options()).unwrap();
        assert!(set.is_empty());
    }

    #[test]
    fn a_row_sets_exactly_one_known_target() {
        let mut two = RuleConfig::new("r", "_source", SetAction::value("x"));
        two.set.insert("_org_id".into(), SetAction::value("y"));
        assert_eq!(locations(&errors(&base().rule(two))), ["rules[0].set"]);
    }

    #[test]
    fn an_action_takes_one_source_of_its_value() {
        let cases = [
            (SetAction::default(), "rules[0].set._source"),
            (
                SetAction {
                    lookup: Some("t".into()),
                    ..SetAction::default()
                },
                "rules[0].set._source",
            ),
            (
                SetAction {
                    value: Some("x".into()),
                    from: Some("f".into()),
                    ..SetAction::default()
                },
                "rules[0].set._source",
            ),
            (SetAction::value(""), "rules[0].set._source.value"),
        ];
        for (action, location) in cases {
            let config = base().rule(RuleConfig::new("r", "_source", action.clone()));
            assert_eq!(locations(&errors(&config)), [location], "{action:?}");
        }
    }

    #[test]
    fn each_op_takes_its_own_operands_only() {
        let mut stray = MatchConfig::equals("host", "x");
        stray.prefix = Some("p".into());
        let mut missing = MatchConfig::equals("host", "x");
        missing.value = None;
        let cases = [
            (stray, vec!["rules[0].match.prefix"]),
            (missing, vec!["rules[0].match.value"]),
            (
                MatchConfig::length("host", Some(5), Some(2)),
                vec!["rules[0].match.min"],
            ),
            (
                MatchConfig::length("host", None, None),
                vec!["rules[0].match"],
            ),
            (
                MatchConfig {
                    op: Some(MatchOp::PrefixAndSuffix),
                    field: Some("host".into()),
                    prefix: Some("a".into()),
                    ..MatchConfig::default()
                },
                vec!["rules[0].match"],
            ),
            (MatchConfig::default(), vec!["rules[0].match"]),
            (
                MatchConfig {
                    field: Some("host".into()),
                    ..MatchConfig::all(vec![MatchConfig::exists("a")])
                },
                vec!["rules[0].match.field"],
            ),
        ];
        for (matcher, expected) in cases {
            let config = base().rule(
                RuleConfig::new("r", "_source", SetAction::value("x")).matching(matcher.clone()),
            );
            assert_eq!(locations(&errors(&config)), expected, "{matcher:?}");
        }
    }

    #[test]
    fn regex_follows_the_profile() {
        let config = base().rule(
            RuleConfig::new("r", "_source", SetAction::value("x"))
                .matching(MatchConfig::regex("host", "^web-[0-9]+$")),
        );
        let off = errors(&config);
        assert_eq!(locations(&off), ["rules[0].match.op"]);
        assert!(off[0].message.contains("allow_regex"));

        let on = CompileOptions::default().with_profile(ProfileConfig {
            allow_regex: true,
            ..ProfileConfig::default()
        });
        assert!(RuleSet::compile_with(&config, &on).is_ok());

        let broken = base().rule(
            RuleConfig::new("r", "_source", SetAction::value("x"))
                .matching(MatchConfig::regex("host", "(")),
        );
        let found: Vec<RuleDiagnostic> = RuleSet::check(&broken, &on)
            .into_iter()
            .filter(RuleDiagnostic::is_error)
            .collect();
        assert_eq!(locations(&found), ["rules[0].match.value"]);
    }

    #[test]
    fn a_cel_condition_error_carries_the_scanner_span() {
        let when = r#"severity == "high" && name.matches("^x")"#;
        let config = base().rule(RuleConfig::new("r", "_source", SetAction::value("x")).when(when));
        let found = errors(&config);
        assert_eq!(locations(&found), ["rules[0].when"]);
        let span = found[0].span.expect("a profile finding has a span");
        assert_eq!(&when[span.start..span.end], "matches");
        assert_eq!(found[0].rule.as_deref(), Some("r"));
    }

    #[test]
    fn match_and_when_together_are_refused() {
        let config = base().rule(
            RuleConfig::new("r", "_source", SetAction::value("x"))
                .matching(MatchConfig::exists("a"))
                .when("a == 1"),
        );
        assert_eq!(locations(&errors(&config)), ["rules[0]"]);
    }

    #[test]
    fn a_lookup_table_is_bounded_and_has_no_empty_key_or_value() {
        let tight = CompileOptions {
            max_lookup_entries: 1,
            ..CompileOptions::default()
        };
        let config = base().lookup("t", [("a", "x"), ("b", "y")]);
        let found = RuleSet::check(&config, &tight);
        assert_eq!(locations(&found), ["lookups.t"]);
        let empty = base().lookup("t", [("", "x"), ("b", "")]);
        assert_eq!(locations(&errors(&empty)), ["lookups.t", "lookups.t"]);
    }

    #[test]
    fn rows_after_an_unconditional_literal_are_reported_shadowed() {
        let config = base()
            .rule(RuleConfig::new(
                "catch-all",
                "_source",
                SetAction::value("main"),
            ))
            .rule(
                RuleConfig::new("late", "_source", SetAction::value("web"))
                    .matching(MatchConfig::starts_with("host", "web-")),
            )
            .rule(RuleConfig::new(
                "other-target",
                "_org_id",
                SetAction::value("o"),
            ));
        let set = RuleSet::compile_with(&config, &options()).unwrap();
        let warnings = set.warnings();
        assert_eq!(locations(warnings), ["rules[1]"]);
        assert_eq!(warnings[0].severity, Severity::Warning);
        assert_eq!(warnings[0].rule.as_deref(), Some("late"));
        assert!(warnings[0].message.contains("catch-all"));
    }

    #[test]
    fn an_earlier_exists_shadows_a_test_that_needs_the_field() {
        let config = base()
            .rule(
                RuleConfig::new("has-host", "_source", SetAction::value("a"))
                    .matching(MatchConfig::exists("host")),
            )
            .rule(
                RuleConfig::new("web", "_source", SetAction::value("b"))
                    .matching(MatchConfig::equals("host", "web")),
            )
            .rule(
                RuleConfig::new("not-web", "_source", SetAction::value("c"))
                    .matching(MatchConfig::equals("host", "web").negate()),
            )
            .rule(
                RuleConfig::new("same", "_source", SetAction::value("d"))
                    .matching(MatchConfig::exists("host")),
            );
        let set = RuleSet::compile_with(&config, &options()).unwrap();
        assert_eq!(locations(set.warnings()), ["rules[1]", "rules[3]"]);
    }

    #[test]
    fn a_from_or_lookup_row_never_shadows() {
        let config = base()
            .rule(RuleConfig::new("copy", "_source", SetAction::from("src")))
            .rule(RuleConfig::new(
                "fallback",
                "_source",
                SetAction::value("main"),
            ));
        let set = RuleSet::compile_with(&config, &options()).unwrap();
        assert!(set.warnings().is_empty(), "{:?}", set.warnings());
    }

    #[test]
    fn a_rule_set_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RuleSet>();
    }

    #[test]
    fn rows_are_grouped_by_target_in_declared_order() {
        let config = base()
            .rule(RuleConfig::new("o1", "_org_id", SetAction::value("a")).when("x == 1"))
            .rule(RuleConfig::new("s1", "_source", SetAction::value("b")).when("x == 2"))
            .rule(RuleConfig::new("o2", "_org_id", SetAction::value("c")).when("x == 3"))
            .rule(RuleConfig::new("s2", "_source", SetAction::value("d")).when("x == 4"));
        let set = RuleSet::compile_with(&config, &options()).unwrap();
        let ids: Vec<&str> = set.rules.iter().map(|r| &*r.id).collect();
        assert_eq!(ids, ["s1", "s2", "o1", "o2"]);
        assert_eq!(set.targets[0].rules, 0..2);
        assert_eq!(set.targets[1].rules, 2..4);
    }

    #[test]
    fn id_rules() {
        for good in ["a", "org-tags", "v1.2", "ns:row/7@x", "A_b"] {
            assert_eq!(id_fault(good), None, "{good}");
        }
        let long = "x".repeat(129);
        for bad in ["", "-a", "a b", "a!", long.as_str()] {
            assert!(id_fault(bad).is_some(), "{bad}");
        }
    }
}
