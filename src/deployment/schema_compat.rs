// Project:   scalo
// File:      src/deployment/schema_compat.rs
// Purpose:   Report the changes between two JSON Schemas that refuse old documents
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Breaking changes between two versions of one JSON Schema.
//!
//! A change is breaking when a document the old schema accepted, or a value an
//! operator already stored, would be refused or ignored under the new one. The
//! check reads the keywords schemars and a hand-written chart schema use; a
//! keyword it does not read is not compared.
//!
//! [`breaking_changes`] reports each one with its path:
//!
//! | Kind | The new schema |
//! | --- | --- |
//! | [`Removed`](SchemaBreakKind::Removed) | lacks a property the old one declared, so a rename reads as one |
//! | [`TypeNarrowed`](SchemaBreakKind::TypeNarrowed) | accepts fewer JSON types |
//! | [`EnumNarrowed`](SchemaBreakKind::EnumNarrowed) | dropped an `enum` or `const` value |
//! | [`ConstraintNarrowed`](SchemaBreakKind::ConstraintNarrowed) | tightened a bound, or added or changed a `pattern`, `format` or `multipleOf` |
//! | [`NewlyRequired`](SchemaBreakKind::NewlyRequired) | requires a property the old one did not |
//! | [`PropertiesClosed`](SchemaBreakKind::PropertiesClosed) | refuses unknown properties the old one let through |
//! | [`BranchRemoved`](SchemaBreakKind::BranchRemoved) | has no `anyOf` or `oneOf` branch accepting an old branch |
//! | [`DialRemoved`](SchemaBreakKind::DialRemoved) | lost a node marked with [`DIAL_KEYWORD`] |
//!
//! A break inside a dial carries [`SchemaBreak::dial`], because a stored dial
//! value outside the new constraints fails every render that carries it.

use std::collections::BTreeSet;
use std::fmt;

use serde_json::{Map, Value};

use super::dials::{DIAL_KEYWORD, pointer};

/// What kind of narrowing a [`SchemaBreak`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaBreakKind {
    /// A property the old schema declared is gone, by removal or rename.
    Removed,
    /// The new schema accepts fewer JSON types.
    TypeNarrowed,
    /// An `enum` or `const` lost a value.
    EnumNarrowed,
    /// A bound tightened, or a `pattern`, `format` or `multipleOf` appeared or changed.
    ConstraintNarrowed,
    /// A property became required.
    NewlyRequired,
    /// Unknown properties were let through and now are not.
    PropertiesClosed,
    /// No branch of the new `anyOf` or `oneOf` accepts an old branch.
    BranchRemoved,
    /// A node marked with [`DIAL_KEYWORD`] lost the marker or was removed.
    DialRemoved,
}

impl fmt::Display for SchemaBreakKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Removed => "removed",
            Self::TypeNarrowed => "type narrowed",
            Self::EnumNarrowed => "enum narrowed",
            Self::ConstraintNarrowed => "constraint narrowed",
            Self::NewlyRequired => "newly required",
            Self::PropertiesClosed => "properties closed",
            Self::BranchRemoved => "branch removed",
            Self::DialRemoved => "dial removed",
        })
    }
}

/// One change that refuses something the old schema accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBreak {
    /// Dotted path of the node: `a.b` for properties, `a[]` for items, `a.*`
    /// for additional properties; empty for the root.
    pub path: String,
    /// The kind of narrowing.
    pub kind: SchemaBreakKind,
    /// The node, or one above it, is an operator dial.
    pub dial: bool,
    /// What changed, in words.
    pub detail: String,
}

impl fmt::Display for SchemaBreak {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = if self.path.is_empty() {
            "(root)"
        } else {
            &self.path
        };
        let dial = if self.dial { " [dial]" } else { "" };
        write!(f, "{path}: {}{dial} -- {}", self.kind, self.detail)
    }
}

/// Every change from `old` to `new` that refuses a document `old` accepted,
/// sorted by path.
///
/// Both are whole schema documents, so a local `$ref` resolves against the
/// document it is in. An empty result means every change is compatible.
#[must_use]
pub fn breaking_changes(old: &Value, new: &Value) -> Vec<SchemaBreak> {
    let mut walker = Walker {
        old_root: old,
        new_root: new,
        seen: BTreeSet::new(),
        out: Vec::new(),
    };
    walker.compare(old, new, "", false);
    walker
        .out
        .sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.detail.cmp(&b.detail)));
    walker.out
}

/// Keywords that bound a value from below; a higher new value narrows.
const LOWER_BOUNDS: [&str; 5] = [
    "minimum",
    "exclusiveMinimum",
    "minLength",
    "minItems",
    "minProperties",
];

/// Keywords that bound a value from above; a lower new value narrows.
const UPPER_BOUNDS: [&str; 5] = [
    "maximum",
    "exclusiveMaximum",
    "maxLength",
    "maxItems",
    "maxProperties",
];

/// Keywords whose presence restricts what a schema accepts.
const RESTRICTING: [&str; 23] = [
    "type",
    "enum",
    "const",
    "required",
    "properties",
    "additionalProperties",
    "items",
    "anyOf",
    "oneOf",
    "allOf",
    "not",
    "pattern",
    "format",
    "multipleOf",
    "uniqueItems",
    "minimum",
    "exclusiveMinimum",
    "minLength",
    "minItems",
    "maximum",
    "exclusiveMaximum",
    "maxLength",
    "maxItems",
];

/// Deepest `$ref` chain followed before a node is taken as it stands.
const MAX_REF_DEPTH: usize = 64;

struct Walker<'a> {
    old_root: &'a Value,
    new_root: &'a Value,
    /// `$ref` pairs already compared, so a recursive schema terminates.
    seen: BTreeSet<(String, String)>,
    out: Vec<SchemaBreak>,
}

impl Walker<'_> {
    fn report(&mut self, path: &str, kind: SchemaBreakKind, dial: bool, detail: String) {
        self.out.push(SchemaBreak {
            path: path.to_string(),
            kind,
            dial,
            detail,
        });
    }

    fn compare(&mut self, old: &Value, new: &Value, path: &str, in_dial: bool) {
        let (old, old_ref) = nullable(self.old_root, old);
        let (new, new_ref) = nullable(self.new_root, new);
        // Recursion passes through a $ref, so a pair seen once is not walked again.
        if (old_ref.is_some() || new_ref.is_some())
            && !self.seen.insert((
                old_ref.unwrap_or_else(|| old.to_string()),
                new_ref.unwrap_or_else(|| new.to_string()),
            ))
        {
            return;
        }
        let dial = in_dial || is_dial(&old);

        if is_dial(&old) && !is_dial(&new) {
            self.report(
                path,
                SchemaBreakKind::DialRemoved,
                true,
                format!("the node no longer carries {DIAL_KEYWORD}"),
            );
        }
        if new == Value::Bool(false) && old != Value::Bool(false) {
            self.report(
                path,
                SchemaBreakKind::TypeNarrowed,
                dial,
                "the new schema accepts nothing".to_string(),
            );
            return;
        }
        let (Some(old_map), Some(new_map)) = (as_schema(&old), as_schema(&new)) else {
            return;
        };
        if !restricts(&new_map) {
            return;
        }
        if !restricts(&old_map) {
            self.report(
                path,
                SchemaBreakKind::TypeNarrowed,
                dial,
                "the old schema accepted any value and the new one does not".to_string(),
            );
            return;
        }

        self.compare_types(&old_map, &new_map, path, dial);
        self.compare_values(&old_map, &new_map, path, dial);
        self.compare_bounds(&old_map, &new_map, path, dial);
        self.compare_required(&old_map, &new_map, path, dial);
        self.compare_properties(&old_map, &new_map, path, dial);
        self.compare_items(&old_map, &new_map, path, dial);
        self.compare_branches(&old, &old_map, &new_map, path, dial);
    }

    fn compare_types(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        dial: bool,
    ) {
        let Some(new_types) = types(new) else {
            return;
        };
        let Some(old_types) = types(old) else {
            self.report(
                path,
                SchemaBreakKind::TypeNarrowed,
                dial,
                format!("any type became {}", join(&new_types)),
            );
            return;
        };
        let lost: Vec<String> = old_types
            .iter()
            .filter(|t| {
                !(new_types.contains(*t) || (*t == "integer" && new_types.contains("number")))
            })
            .cloned()
            .collect();
        if !lost.is_empty() {
            self.report(
                path,
                SchemaBreakKind::TypeNarrowed,
                dial,
                format!("{} became {}", join(&old_types), join(&new_types)),
            );
        }
    }

    fn compare_values(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        dial: bool,
    ) {
        let Some(allowed) = allowed_values(new) else {
            return;
        };
        match allowed_values(old) {
            None => self.report(
                path,
                SchemaBreakKind::EnumNarrowed,
                dial,
                format!("any value became one of {}", Value::Array(allowed)),
            ),
            Some(before) => {
                let lost: Vec<Value> = before
                    .into_iter()
                    .filter(|v| !allowed.contains(v))
                    .collect();
                if !lost.is_empty() {
                    self.report(
                        path,
                        SchemaBreakKind::EnumNarrowed,
                        dial,
                        format!("{} no longer allowed", Value::Array(lost)),
                    );
                }
            }
        }
    }

    fn compare_bounds(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        dial: bool,
    ) {
        for key in LOWER_BOUNDS {
            if let Some(after) = new.get(key).and_then(Value::as_f64) {
                match old.get(key).and_then(Value::as_f64) {
                    Some(before) if after <= before => {}
                    before => self.narrowed(path, dial, key, before, after),
                }
            }
        }
        for key in UPPER_BOUNDS {
            if let Some(after) = new.get(key).and_then(Value::as_f64) {
                match old.get(key).and_then(Value::as_f64) {
                    Some(before) if after >= before => {}
                    before => self.narrowed(path, dial, key, before, after),
                }
            }
        }
        if let Some(after) = new.get("multipleOf").and_then(Value::as_f64) {
            let before = old.get("multipleOf").and_then(Value::as_f64);
            let kept = before
                .is_some_and(|b| after != 0.0 && ((b / after).round() - b / after).abs() < 1e-9);
            if !kept {
                self.narrowed(path, dial, "multipleOf", before, after);
            }
        }
        for key in ["pattern", "format"] {
            if let Some(after) = new.get(key)
                && old.get(key) != Some(after)
            {
                let before = old
                    .get(key)
                    .map_or_else(|| "none".to_string(), Value::to_string);
                self.report(
                    path,
                    SchemaBreakKind::ConstraintNarrowed,
                    dial,
                    format!("{key} {before} became {after}"),
                );
            }
        }
        if new.get("uniqueItems") == Some(&Value::Bool(true))
            && old.get("uniqueItems") != Some(&Value::Bool(true))
        {
            self.report(
                path,
                SchemaBreakKind::ConstraintNarrowed,
                dial,
                "items must now be unique".to_string(),
            );
        }
    }

    fn narrowed(&mut self, path: &str, dial: bool, key: &str, before: Option<f64>, after: f64) {
        let before = before.map_or_else(|| "none".to_string(), |b| b.to_string());
        self.report(
            path,
            SchemaBreakKind::ConstraintNarrowed,
            dial,
            format!("{key} {before} became {after}"),
        );
    }

    fn compare_required(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        dial: bool,
    ) {
        let before = string_set(old.get("required"));
        for key in string_set(new.get("required")).difference(&before) {
            self.report(
                &child(path, key),
                SchemaBreakKind::NewlyRequired,
                dial,
                "a document without it is now refused".to_string(),
            );
        }
    }

    fn compare_properties(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        dial: bool,
    ) {
        let empty = Map::new();
        let old_props = old
            .get("properties")
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let new_props = new
            .get("properties")
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        for (key, old_child) in old_props {
            let at = child(path, key);
            if let Some(new_child) = new_props.get(key) {
                self.compare(old_child, new_child, &at, dial);
            } else {
                let (target, _) = nullable(self.old_root, old_child);
                let was_dial = dial || is_dial(&target);
                let kind = if was_dial {
                    SchemaBreakKind::DialRemoved
                } else {
                    SchemaBreakKind::Removed
                };
                self.report(&at, kind, was_dial, "the property is gone".to_string());
            }
        }
        match (
            old.get("additionalProperties"),
            new.get("additionalProperties"),
        ) {
            (before, Some(Value::Bool(false))) if before != Some(&Value::Bool(false)) => self
                .report(
                    path,
                    SchemaBreakKind::PropertiesClosed,
                    dial,
                    "unknown properties are now refused".to_string(),
                ),
            (None | Some(Value::Bool(true)), Some(after @ Value::Object(_))) => {
                self.compare(&Value::Bool(true), after, &format!("{path}.*"), dial);
            }
            (Some(before @ Value::Object(_)), Some(after @ Value::Object(_))) => {
                self.compare(before, after, &format!("{path}.*"), dial);
            }
            _ => {}
        }
    }

    fn compare_items(
        &mut self,
        old: &Map<String, Value>,
        new: &Map<String, Value>,
        path: &str,
        dial: bool,
    ) {
        let Some(after) = new.get("items") else {
            return;
        };
        let before = old.get("items").cloned().unwrap_or(Value::Bool(true));
        self.compare(&before, after, &format!("{path}[]"), dial);
    }

    /// `anyOf` and `oneOf` are matched by acceptance, not position: each old
    /// branch needs some new branch that accepts all it did. `allOf` narrows
    /// only by what a new conjunct adds.
    fn compare_branches(
        &mut self,
        old: &Value,
        old_map: &Map<String, Value>,
        new_map: &Map<String, Value>,
        path: &str,
        dial: bool,
    ) {
        let new_branches = branches(new_map);
        if !new_branches.is_empty() {
            let old_branches = branches(old_map);
            let olds: Vec<Value> = if old_branches.is_empty() {
                vec![without_branches(old)]
            } else {
                old_branches
            };
            for (index, before) in olds.iter().enumerate() {
                let accepted = new_branches
                    .iter()
                    .any(|after| self.trial(before, after, path, dial).is_empty());
                if !accepted {
                    self.report(
                        path,
                        SchemaBreakKind::BranchRemoved,
                        dial,
                        format!("no new branch accepts old branch {index}"),
                    );
                }
            }
        }
        for conjunct in new_map
            .get("allOf")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let before = without_branches(old);
            self.compare(&before, conjunct, path, dial);
        }
    }

    /// The breaks `old` to `new` would report, without recording them.
    fn trial(&self, old: &Value, new: &Value, path: &str, dial: bool) -> Vec<SchemaBreak> {
        let mut walker = Walker {
            old_root: self.old_root,
            new_root: self.new_root,
            seen: self.seen.clone(),
            out: Vec::new(),
        };
        walker.compare(old, new, path, dial);
        walker.out
    }
}

/// `node` with its `$ref` chain followed, sibling keys winning, and the last
/// reference followed.
fn resolve(root: &Value, node: &Value) -> (Value, Option<String>) {
    let mut current = node.clone();
    let mut last = None;
    for _ in 0..MAX_REF_DEPTH {
        let Some(reference) = current
            .get("$ref")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            break;
        };
        let Ok(target) = pointer(root, &reference) else {
            break;
        };
        let mut merged = target.as_object().cloned().unwrap_or_default();
        if let Some(map) = current.as_object() {
            for (key, value) in map.iter().filter(|(key, _)| key.as_str() != "$ref") {
                merged.insert(key.clone(), value.clone());
            }
        }
        current = Value::Object(merged);
        last = Some(reference);
    }
    (current, last)
}

/// `node` resolved, with an `anyOf` of one schema and `{"type": "null"}` read
/// as that schema plus `null`, which is how schemars writes an `Option`. The
/// reference is the last `$ref` followed on the way.
fn nullable(root: &Value, node: &Value) -> (Value, Option<String>) {
    let (node, outer_ref) = resolve(root, node);
    let Some(map) = node.as_object() else {
        return (node, outer_ref);
    };
    let Some([first, second]) = map
        .get("anyOf")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
    else {
        return (node, outer_ref);
    };
    let is_null = |v: &Value| v.get("type") == Some(&Value::String("null".into()));
    let other = match (is_null(first), is_null(second)) {
        (true, false) => second,
        (false, true) => first,
        _ => return (node, outer_ref),
    };
    let (base, inner_ref) = resolve(root, other);
    let mut merged = base.as_object().cloned().unwrap_or_default();
    for (key, value) in map.iter().filter(|(key, _)| key.as_str() != "anyOf") {
        merged.insert(key.clone(), value.clone());
    }
    if let Some(mut set) = types(&merged) {
        set.insert("null".to_string());
        let list: Vec<Value> = set.into_iter().map(Value::String).collect();
        merged.insert("type".to_string(), Value::Array(list));
    }
    (Value::Object(merged), inner_ref.or(outer_ref))
}

/// The node as a keyword map; `true` reads as the empty schema.
fn as_schema(node: &Value) -> Option<Map<String, Value>> {
    match node {
        Value::Bool(true) => Some(Map::new()),
        Value::Object(map) => Some(map.clone()),
        _ => None,
    }
}

fn restricts(map: &Map<String, Value>) -> bool {
    RESTRICTING.iter().any(|key| match (*key, map.get(*key)) {
        (_, None) => false,
        ("required", Some(Value::Array(items))) => !items.is_empty(),
        ("properties", Some(Value::Object(props))) => !props.is_empty(),
        ("uniqueItems", Some(value)) => value == &Value::Bool(true),
        ("additionalProperties" | "items", Some(Value::Bool(open))) => !open,
        _ => true,
    })
}

fn is_dial(node: &Value) -> bool {
    node.get(DIAL_KEYWORD).is_some()
}

/// The JSON types a node allows; `None` when it names none, so allows all.
fn types(map: &Map<String, Value>) -> Option<BTreeSet<String>> {
    match map.get("type")? {
        Value::String(t) => Some(BTreeSet::from([t.clone()])),
        Value::Array(list) => Some(
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
        ),
        _ => None,
    }
}

/// The values an `enum` or `const` allows; `None` when neither is set.
fn allowed_values(map: &Map<String, Value>) -> Option<Vec<Value>> {
    if let Some(value) = map.get("const") {
        return Some(vec![value.clone()]);
    }
    map.get("enum").and_then(Value::as_array).cloned()
}

fn string_set(value: Option<&Value>) -> BTreeSet<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn branches(map: &Map<String, Value>) -> Vec<Value> {
    ["anyOf", "oneOf"]
        .iter()
        .filter_map(|key| map.get(*key).and_then(Value::as_array))
        .flatten()
        .cloned()
        .collect()
}

fn without_branches(node: &Value) -> Value {
    let mut map = node.as_object().cloned().unwrap_or_default();
    for key in ["anyOf", "oneOf", "allOf"] {
        map.remove(key);
    }
    Value::Object(map)
}

fn child(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

fn join(set: &BTreeSet<String>) -> String {
    set.iter().cloned().collect::<Vec<_>>().join("|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn kinds(old: &Value, new: &Value) -> Vec<(String, SchemaBreakKind)> {
        breaking_changes(old, new)
            .into_iter()
            .map(|b| (b.path, b.kind))
            .collect()
    }

    fn object(properties: Value) -> Value {
        json!({ "type": "object", "properties": properties })
    }

    #[test]
    fn an_identical_schema_has_no_breaks() {
        let mut schema = object(json!({
            "a": { "type": "string", "pattern": "^x" },
            "b": { "$ref": "#/$defs/B" }
        }));
        schema["$defs"] = json!({ "B": { "type": "integer", "minimum": 1 } });
        assert_eq!(breaking_changes(&schema, &schema), vec![]);
    }

    #[test]
    fn a_removed_field_is_breaking() {
        let old = object(json!({ "a": { "type": "string" }, "b": { "type": "string" } }));
        let new = object(json!({ "a": { "type": "string" } }));
        assert_eq!(
            kinds(&old, &new),
            [("b".to_string(), SchemaBreakKind::Removed)]
        );
    }

    #[test]
    fn a_renamed_field_is_breaking() {
        let old = object(json!({ "grace_seconds": { "type": "integer" } }));
        let new = object(json!({ "termination_grace_seconds": { "type": "integer" } }));
        assert_eq!(
            kinds(&old, &new),
            [("grace_seconds".to_string(), SchemaBreakKind::Removed)]
        );
    }

    #[test]
    fn a_narrowed_type_is_breaking() {
        for (before, after) in [
            (json!("number"), json!("integer")),
            (json!(["string", "null"]), json!("string")),
            (json!("string"), json!("boolean")),
        ] {
            let old = object(json!({ "a": { "type": before } }));
            let new = object(json!({ "a": { "type": after } }));
            assert_eq!(
                kinds(&old, &new),
                [("a".to_string(), SchemaBreakKind::TypeNarrowed)],
                "{before} -> {after}"
            );
        }
    }

    #[test]
    fn an_option_that_loses_null_is_a_narrowed_type() {
        let old =
            object(json!({ "a": { "anyOf": [{ "$ref": "#/$defs/A" }, { "type": "null" }] } }));
        let new = object(json!({ "a": { "$ref": "#/$defs/A" } }));
        let defs = json!({ "A": { "type": "object", "properties": {} } });
        let mut old = old;
        let mut new = new;
        old["$defs"] = defs.clone();
        new["$defs"] = defs;
        assert_eq!(
            kinds(&old, &new),
            [("a".to_string(), SchemaBreakKind::TypeNarrowed)]
        );
    }

    #[test]
    fn a_narrowed_enum_is_breaking() {
        let old = object(json!({ "tier": { "type": "string", "enum": ["big", "small"] } }));
        let new = object(json!({ "tier": { "type": "string", "enum": ["big"] } }));
        assert_eq!(
            kinds(&old, &new),
            [("tier".to_string(), SchemaBreakKind::EnumNarrowed)]
        );
    }

    #[test]
    fn a_new_required_field_is_breaking() {
        let old = object(json!({ "a": { "type": "string" } }));
        let mut new = old.clone();
        new["required"] = json!(["a"]);
        assert_eq!(
            kinds(&old, &new),
            [("a".to_string(), SchemaBreakKind::NewlyRequired)]
        );
    }

    #[test]
    fn a_tightened_bound_or_added_pattern_is_breaking() {
        for (before, after) in [
            (
                json!({ "type": "integer", "minimum": 1 }),
                json!({ "type": "integer", "minimum": 2 }),
            ),
            (
                json!({ "type": "integer", "maximum": 10 }),
                json!({ "type": "integer", "maximum": 9 }),
            ),
            (
                json!({ "type": "integer" }),
                json!({ "type": "integer", "maximum": 9 }),
            ),
            (
                json!({ "type": "string" }),
                json!({ "type": "string", "pattern": "^a" }),
            ),
            (
                json!({ "type": "string", "maxLength": 9 }),
                json!({ "type": "string", "maxLength": 8 }),
            ),
            (
                json!({ "type": "integer", "multipleOf": 2 }),
                json!({ "type": "integer", "multipleOf": 3 }),
            ),
        ] {
            let old = object(json!({ "a": before }));
            let new = object(json!({ "a": after }));
            assert_eq!(
                kinds(&old, &new),
                [("a".to_string(), SchemaBreakKind::ConstraintNarrowed)],
                "{before} -> {after}"
            );
        }
    }

    #[test]
    fn closing_additional_properties_is_breaking() {
        let old = object(json!({ "a": { "type": "string" } }));
        let mut new = old.clone();
        new["additionalProperties"] = json!(false);
        assert_eq!(
            kinds(&old, &new),
            [(String::new(), SchemaBreakKind::PropertiesClosed)]
        );
    }

    #[test]
    fn a_dial_constraint_narrowed_is_breaking_and_marked_as_a_dial() {
        let old =
            object(json!({ "rows": { "type": "integer", "maximum": 100, "x-scalo-dial": "big" } }));
        let new =
            object(json!({ "rows": { "type": "integer", "maximum": 50, "x-scalo-dial": "big" } }));
        let breaks = breaking_changes(&old, &new);
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert_eq!(breaks[0].kind, SchemaBreakKind::ConstraintNarrowed);
        assert!(breaks[0].dial);
        assert_eq!(
            breaks[0].to_string(),
            "rows: constraint narrowed [dial] -- maximum 100 became 50"
        );
    }

    #[test]
    fn a_removed_dial_is_breaking() {
        let old = object(json!({
            "rows": { "type": "integer", "x-scalo-dial": "big" },
            "age": { "$ref": "#/$defs/Age" }
        }));
        let mut old = old;
        old["$defs"] = json!({ "Age": { "type": "integer", "x-scalo-dial": "small" } });
        let unmarked =
            object(json!({ "rows": { "type": "integer" }, "age": { "type": "integer" } }));
        assert_eq!(
            kinds(&old, &unmarked),
            [
                ("age".to_string(), SchemaBreakKind::DialRemoved),
                ("rows".to_string(), SchemaBreakKind::DialRemoved)
            ]
        );
        let gone = object(json!({}));
        assert_eq!(
            kinds(&old, &gone),
            [
                ("age".to_string(), SchemaBreakKind::DialRemoved),
                ("rows".to_string(), SchemaBreakKind::DialRemoved)
            ]
        );
    }

    #[test]
    fn a_removed_branch_is_breaking() {
        let old = object(json!({ "when": { "oneOf": [
            { "type": "object", "properties": { "kind": { "const": "enabled" } } },
            { "type": "object", "properties": { "kind": { "const": "equals" } } }
        ] } }));
        let new = object(json!({ "when": { "oneOf": [
            { "type": "object", "properties": { "kind": { "const": "equals" } } }
        ] } }));
        assert_eq!(
            kinds(&old, &new),
            [("when".to_string(), SchemaBreakKind::BranchRemoved)]
        );
    }

    #[test]
    fn widening_changes_are_compatible() {
        let old = json!({
            "type": "object",
            "required": ["a"],
            "properties": {
                "a": { "type": "integer", "minimum": 1, "maximum": 10, "x-scalo-dial": "big" },
                "b": { "type": "string", "enum": ["x"], "pattern": "^x" },
                "c": { "oneOf": [{ "const": "one" }] },
                "d": { "type": "integer" }
            }
        });
        let new = json!({
            "type": "object",
            "properties": {
                "a": { "type": "number", "minimum": 0, "maximum": 20, "x-scalo-dial": "small" },
                "b": { "type": ["string", "null"], "enum": ["x", "y"], "pattern": "^x" },
                "c": { "oneOf": [{ "const": "two" }, { "const": "one" }] },
                "d": true,
                "e": { "type": "string" }
            }
        });
        assert_eq!(breaking_changes(&old, &new), vec![]);
    }

    #[test]
    fn a_recursive_schema_terminates() {
        let schema = json!({
            "$ref": "#/$defs/Node",
            "$defs": { "Node": { "type": "object", "properties": {
                "child": { "anyOf": [{ "$ref": "#/$defs/Node" }, { "type": "null" }] }
            } } }
        });
        assert_eq!(breaking_changes(&schema, &schema), vec![]);
    }
}
