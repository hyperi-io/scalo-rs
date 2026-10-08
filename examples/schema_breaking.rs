// Project:   scalo
// File:      examples/schema_breaking.rs
// Purpose:   Exit non-zero when a JSON Schema refuses what its previous version accepted
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Compare two versions of one JSON Schema and list every breaking change.
//!
//! Exit 0 when the new schema accepts everything the old one did, 1 when it
//! does not, 2 when either file cannot be read as JSON.
//!
//! ```bash
//! cargo run --example schema_breaking --no-default-features --features deployment -- old.json new.json
//! ```

use std::process::ExitCode;

use scalo::deployment::breaking_changes;

fn read(path: &str) -> Result<serde_json::Value, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [old, new] = args.as_slice() else {
        eprintln!("usage: schema_breaking <old.json> <new.json>");
        return ExitCode::from(2);
    };
    let (old_schema, new_schema) = match (read(old), read(new)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let breaks = breaking_changes(&old_schema, &new_schema);
    if breaks.is_empty() {
        println!("{new}: no breaking change from {old}");
        return ExitCode::SUCCESS;
    }
    eprintln!("{new}: {} breaking change(s) from {old}:", breaks.len());
    for found in &breaks {
        eprintln!("  {found}");
    }
    ExitCode::FAILURE
}
