// Project:   scalo
// File:      src/deployment/dials.rs
// Purpose:   Find the operator dials an app's config schema marks
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Operator dials in an app's config schema.
//!
//! A dial is a config setting an operator is expected to tune per deployment.
//! The app marks it in its `Config` with the [`DIAL_KEYWORD`] JSON Schema
//! keyword, set to one of [`DIAL_TIERS`]:
//!
//! ```rust,ignore
//! #[derive(schemars::JsonSchema)]
//! struct Buffer {
//!     /// Rows held before a flush.
//!     #[schemars(extend("x-scalo-dial" = "big"), range(min = 1, max = 10_000_000))]
//!     flush_rows: u64,
//! }
//! ```
//!
//! Constraints stay plain JSON Schema (`minimum`, `maximum`, `enum`,
//! `pattern`), so a chart's `values.schema.json` copies the marked node whole.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

/// The JSON Schema keyword that marks a config setting as an operator dial.
pub const DIAL_KEYWORD: &str = "x-scalo-dial";

/// The values [`DIAL_KEYWORD`] takes: a setting most deployments tune, and one
/// a few do.
pub const DIAL_TIERS: [&str; 2] = ["big", "small"];

/// Why a config schema's dials cannot be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DialError {
    /// A marker holds something other than one of [`DIAL_TIERS`].
    #[error("config.{path}: {DIAL_KEYWORD} is {value}, and it must be \"big\" or \"small\"")]
    BadTier {
        /// Dotted path of the marked node.
        path: String,
        /// The marker's value, as JSON.
        value: String,
    },
    /// A name on a dial's path is not a Helm value key a chart can write.
    #[error(
        "config.{path:?}: the dial path holds {name:?}, and a dial name is one or more letters, \
         digits, '_' or '-', because it becomes a key in the chart's values"
    )]
    BadName {
        /// Dotted path of the marked node.
        path: String,
        /// The first name on the path that is not a value key.
        name: String,
    },
    /// A `$ref` is not local, points at nothing, or refers to itself.
    #[error("config_schema $ref {reference:?} {reason}")]
    BadRef {
        /// The reference.
        reference: String,
        /// What is wrong with it.
        reason: &'static str,
    },
}

/// Every node of `config_schema` marked with [`DIAL_KEYWORD`], by dotted path.
///
/// A marked node is one dial, so nothing beneath it is searched. `allOf`,
/// `anyOf` and `oneOf` branches sit at their parent's path, and the first node
/// found at a path wins. Each returned node has its local `$ref`s inlined.
///
/// # Errors
///
/// [`DialError`] when a marker is not `big` or `small`, or a `$ref` the walk
/// follows is broken.
pub fn dials(config_schema: &Value) -> Result<BTreeMap<String, Value>, DialError> {
    let mut found = BTreeMap::new();
    walk(
        config_schema,
        config_schema,
        &mut Vec::new(),
        &BTreeSet::new(),
        &mut found,
    )?;
    Ok(found)
}

fn walk(
    root: &Value,
    node: &Value,
    path: &mut Vec<String>,
    seen: &BTreeSet<String>,
    found: &mut BTreeMap<String, Value>,
) -> Result<(), DialError> {
    let Some(map) = node.as_object() else {
        return Ok(());
    };
    if let Some(tier) = map.get(DIAL_KEYWORD) {
        if !tier.as_str().is_some_and(|t| DIAL_TIERS.contains(&t)) {
            return Err(DialError::BadTier {
                path: path.join("."),
                value: tier.to_string(),
            });
        }
        if let Some(name) = path.iter().find(|name| !is_value_key(name)) {
            return Err(DialError::BadName {
                path: path.join("."),
                name: name.clone(),
            });
        }
        let inlined = inline(root, node, &BTreeSet::new())?;
        found.entry(path.join(".")).or_insert(inlined);
        return Ok(());
    }
    if let Some(reference) = map.get("$ref").and_then(Value::as_str)
        && !seen.contains(reference)
    {
        let mut seen = seen.clone();
        seen.insert(reference.to_string());
        walk(root, pointer(root, reference)?, path, &seen, found)?;
    }
    if let Some(properties) = map.get("properties").and_then(Value::as_object) {
        let mut names: Vec<&String> = properties.keys().collect();
        names.sort();
        for name in names {
            path.push(name.clone());
            walk(root, &properties[name], path, seen, found)?;
            path.pop();
        }
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        for branch in map.get(key).and_then(Value::as_array).into_iter().flatten() {
            walk(root, branch, path, seen, found)?;
        }
    }
    Ok(())
}

/// Whether `name` can be a Helm value key: a `.` would split it, and a newline or space would break the values file line it is written on.
fn is_value_key(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The node a local `$ref` names in `root`.
pub(crate) fn pointer<'a>(root: &'a Value, reference: &str) -> Result<&'a Value, DialError> {
    let bad = |reason| DialError::BadRef {
        reference: reference.to_string(),
        reason,
    };
    let Some(fragment) = reference.strip_prefix('#') else {
        return Err(bad("is not local to the schema"));
    };
    let mut node = root;
    for part in fragment.split('/').filter(|part| !part.is_empty()) {
        let key = part.replace("~1", "/").replace("~0", "~");
        node = node.get(&key).ok_or_else(|| bad("points at nothing"))?;
    }
    Ok(node)
}

/// `node` with every local `$ref` replaced by its target, sibling keys winning.
fn inline(root: &Value, node: &Value, seen: &BTreeSet<String>) -> Result<Value, DialError> {
    match node {
        Value::Array(items) => items
            .iter()
            .map(|item| inline(root, item, seen))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(map) => {
            let mut out = Map::new();
            if let Some(reference) = map.get("$ref").and_then(Value::as_str) {
                if seen.contains(reference) {
                    return Err(DialError::BadRef {
                        reference: reference.to_string(),
                        reason: "refers to itself",
                    });
                }
                let mut seen = seen.clone();
                seen.insert(reference.to_string());
                if let Value::Object(target) = inline(root, pointer(root, reference)?, &seen)? {
                    out = target;
                }
            }
            for (key, value) in map.iter().filter(|(key, _)| key.as_str() != "$ref") {
                out.insert(key.clone(), inline(root, value, seen)?);
            }
            Ok(Value::Object(out))
        }
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "buffer": { "$ref": "#/$defs/Buffer" },
                "level": { "type": "string", "x-scalo-dial": "small", "enum": ["info", "debug"] },
                "brokers": { "type": "array", "items": { "type": "string" } },
                "limit": {
                    "anyOf": [{ "type": "integer", "minimum": 1 }, { "type": "null" }],
                    "x-scalo-dial": "big"
                }
            },
            "$defs": {
                "Buffer": {
                    "type": "object",
                    "properties": {
                        "rows": { "type": "integer", "minimum": 1, "maximum": 100, "x-scalo-dial": "big" },
                        "age": { "$ref": "#/$defs/Seconds", "x-scalo-dial": "small", "default": 5 }
                    }
                },
                "Seconds": { "type": "integer", "minimum": 0 }
            }
        })
    }

    #[test]
    fn marked_nodes_are_found_by_dotted_path_with_refs_inlined() {
        let found = dials(&schema()).unwrap();
        let paths: Vec<&str> = found.keys().map(String::as_str).collect();
        assert_eq!(paths, ["buffer.age", "buffer.rows", "level", "limit"]);
        assert_eq!(
            found["buffer.age"],
            json!({ "type": "integer", "minimum": 0, "x-scalo-dial": "small", "default": 5 })
        );
        assert_eq!(found["buffer.rows"]["maximum"], 100);
        assert_eq!(found["limit"]["anyOf"][0]["minimum"], 1);
    }

    #[test]
    fn a_schema_without_markers_has_no_dials() {
        let found =
            dials(&json!({ "type": "object", "properties": { "a": { "type": "string" } } }));
        assert_eq!(found.unwrap(), BTreeMap::new());
    }

    #[test]
    fn a_dial_whose_path_holds_a_name_that_is_not_a_value_key_is_refused() {
        for bad in ["batch.size", "flush rows", "rows\n", "", "r\u{f6}ws"] {
            let schema = json!({
                "properties": { "buffer": { "properties": {
                    bad: { "type": "integer", "x-scalo-dial": "big" }
                } } }
            });
            match dials(&schema) {
                Err(DialError::BadName { name, .. }) => assert_eq!(name, bad),
                other => panic!("{bad:?} passed: {other:?}"),
            }
        }
        let fine = json!({ "properties": { "flush_rows": { "x-scalo-dial": "big" },
                                           "max-age": { "x-scalo-dial": "small" },
                                           "odd name": { "type": "string" } } });
        let paths: Vec<String> = dials(&fine).unwrap().into_keys().collect();
        assert_eq!(paths, ["flush_rows", "max-age"]);
    }

    #[test]
    fn a_marker_outside_the_tiers_is_refused() {
        for bad in [json!("medium"), json!(true), json!(1)] {
            let schema =
                json!({ "properties": { "a": { "type": "string", "x-scalo-dial": bad } } });
            match dials(&schema) {
                Err(DialError::BadTier { path, .. }) => assert_eq!(path, "a"),
                other => panic!("{bad} passed: {other:?}"),
            }
        }
    }

    #[test]
    fn a_broken_or_remote_ref_is_refused() {
        for reference in ["#/$defs/Missing", "https://example.com/schema.json"] {
            let schema = json!({ "properties": { "a": { "$ref": reference } } });
            assert!(
                matches!(dials(&schema), Err(DialError::BadRef { .. })),
                "{reference} passed"
            );
        }
    }

    #[test]
    fn a_dial_referring_to_itself_is_refused() {
        let schema = json!({
            "properties": { "a": { "$ref": "#/$defs/Loop", "x-scalo-dial": "big" } },
            "$defs": { "Loop": { "properties": { "next": { "$ref": "#/$defs/Loop" } } } }
        });
        assert!(matches!(dials(&schema), Err(DialError::BadRef { .. })));
    }

    #[test]
    fn a_recursive_schema_without_markers_terminates() {
        let schema = json!({
            "$ref": "#/$defs/Node",
            "$defs": { "Node": { "properties": { "child": { "$ref": "#/$defs/Node" } } } }
        });
        assert_eq!(dials(&schema).unwrap(), BTreeMap::new());
    }
}
