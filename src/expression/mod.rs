// Project:   scalo
// File:      src/expression/mod.rs
// Purpose:   CEL expression evaluation for data-plane components
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CEL expression evaluation -- compile, evaluate, validate.
//!
//! Provides a profile-restricted CEL expression evaluator built on the
//! [`cel`] crate (renamed from `cel-interpreter`). Both Python (via
//! `common-expression-language` PyO3 bindings) and Rust use the **same**
//! underlying Rust crate, ensuring identical parsing and evaluation
//! semantics across all data-plane components.
//!
//! # Expression Profile
//!
//! Only a high-performance subset of CEL is allowed:
//!
//! | Category | Examples |
//! |----------|---------|
//! | Comparison | `==`, `!=`, `<`, `<=`, `>`, `>=` |
//! | Logical | `&&`, `\|\|`, `!` |
//! | Membership | `in` (list elements, map keys) |
//! | String | `contains()`, `startsWith()`, `endsWith()` (strings only) |
//! | Existence | `has()` |
//! | Size | `size()` |
//! | Ternary | `? :` |
//! | Type casts | `int()`, `double()`, `string()`, `bool()` |
//! | Arithmetic | `+`, `-`, `*`, `/`, `%` |
//!
//! **Restricted (blocked by default, opt-in via [`ProfileConfig`]):**
//! - Regex: `matches()` -- unbounded cost per record
//! - Iteration: `map()`, `filter()`, `exists()`, `all()`, `exists_one()` and
//!   `existsOne()` -- O(n) per collection
//! - Time: `timestamp()`, `duration()` -- ClickHouse handles natively
//!
//! # Usage
//!
//! ```rust
//! use scalo::expression::{compile, evaluate, evaluate_condition, validate};
//! use std::collections::HashMap;
//! use serde_json::json;
//!
//! // Validate (returns errors list, empty if valid)
//! assert!(validate(r#"severity == "critical""#).is_empty());
//!
//! // Diagnose: the same findings, each with the byte span it covers
//! let expr = r#"name.matches("^x")"#;
//! let found = scalo::expression::diagnose(expr);
//! assert_eq!(found[0].span.map(|s| &expr[s.start..s.end]), Some("matches"));
//!
//! // One-shot evaluation
//! let mut data = HashMap::new();
//! data.insert("amount".into(), json!(15000));
//! let result = evaluate("amount > 10000", &data).unwrap();
//! assert_eq!(result, true.into());
//!
//! // Boolean condition (missing fields -> false)
//! assert!(!evaluate_condition(r#"severity == "critical""#, &HashMap::new()));
//!
//! // Compile for hot-path reuse
//! let program = compile("score > threshold").unwrap();
//! // ... program.execute(&context) per record
//! ```

pub mod diagnostic;
pub mod error;
pub mod evaluator;
pub mod profile;

pub use diagnostic::{Diagnostic, Span};
pub use error::{ExpressionError, ExpressionResult};
pub use evaluator::{
    build_context, compile, compile_with_config, diagnose, diagnose_with_config, evaluate,
    evaluate_condition, validate, validate_with_config,
};
pub use profile::{
    ALLOWED_FUNCTIONS, DISALLOWED_FUNCTIONS, ProfileConfig, RESTRICTED_ITERATION, RESTRICTED_REGEX,
    RESTRICTED_TIME, check_profile_with_config, scan_profile,
};
