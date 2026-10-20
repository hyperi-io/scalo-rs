// Project:   scalo
// File:      src/rules/mod.rs
// Purpose:   Ordered first-match rules that set target fields on a record
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! An ordered rule set: for each target field in turn, the first row in
//! declared order that matches the record and resolves a value sets it.
//!
//! A row tests the record with a native test (`equals`, `starts_with`,
//! `ends_with`, `contains`, `prefix_and_suffix`, `length`, `exists`, `regex`,
//! each with `negate`, combined with `all`) or a CEL Fast condition, and finds
//! its value as a literal, a copy of a field, or a field mapped through a
//! bounded lookup table. A row whose copy or lookup finds nothing does not
//! match, so the next row is tried.
//!
//! Targets are evaluated in the order the set lists them, so a row for a later
//! target can test an earlier target's new value. A [`Clamp`] then holds one
//! target to a per-call allowed set, replacing anything outside it with a
//! default and counting the replacement.
//!
//! ```rust
//! use std::collections::HashSet;
//! use scalo::rules::{
//!     Clamp, CompileOptions, JsonPayload, MatchConfig, RuleConfig, RuleSet, RuleSetConfig,
//!     SetAction,
//! };
//!
//! let config = RuleSetConfig::new("ingest", ["_source", "_org_id"])
//!     .rule(
//!         RuleConfig::new("web", "_source", SetAction::value("web"))
//!             .matching(MatchConfig::starts_with("host", "web-")),
//!     )
//!     .rule(
//!         RuleConfig::new("web-org", "_org_id", SetAction::value("acme"))
//!             .matching(MatchConfig::equals("_source", "web")),
//!     );
//! let rules = RuleSet::compile_with(&config, &CompileOptions::default())?;
//!
//! let record = JsonPayload::new(br#"{"host":"web-01"}"#).ok_or("too deep")?;
//! let org = rules.target("_org_id").ok_or("no target")?;
//! let allowed: HashSet<String> = ["acme".to_string(), "default".to_string()].into();
//! let outcome = rules.evaluate_clamped(&record, &Clamp::new(org, &allowed, "default"));
//! assert_eq!(outcome.value(org), Some("acme"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Compile errors and warnings are [`RuleDiagnostic`]s naming the config
//! location, the rule id, and for a CEL condition the byte span the scanner or
//! parser found. Each row counts its hits, read with [`RuleSet::usage`] and,
//! with the `metrics` feature, as `rules_hits_total` and `rules_clamped_total`.

mod compile;
mod config;
mod error;
mod eval;
mod field;
#[cfg(feature = "config-reload")]
mod reload;
mod usage;

pub use compile::RuleSet;
pub use config::{
    DEFAULT_RULE_SET_NAME, MatchConfig, MatchOp, RuleConfig, RuleSetConfig, SetAction,
};
pub use error::{RuleDiagnostic, RulesError, Severity};
pub use eval::{Assignment, Batch, Clamp, ClampOutcome, Outcome, RuleRef, RuleUsage, TargetId};
pub use field::{FieldPath, FieldSource, JsonPayload, Lookup};
#[cfg(feature = "config-reload")]
#[cfg_attr(docsrs, doc(cfg(feature = "config-reload")))]
pub use reload::reloader;
pub use usage::RECORD_LABEL;

use crate::expression::ProfileConfig;

/// The most target fields one set assigns. An outcome holds them inline, so
/// evaluation allocates nothing for them.
pub const MAX_TARGETS: usize = 8;

/// Entries one lookup table holds unless the options say otherwise.
pub const DEFAULT_MAX_LOOKUP_ENTRIES: usize = 100_000;

/// What a compile allows.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CompileOptions {
    /// The CEL Fast profile `when` conditions compile under, and whether the
    /// `regex` test is allowed (`allow_regex`).
    pub profile: ProfileConfig,
    /// Most entries one lookup table holds.
    pub max_lookup_entries: usize,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            profile: ProfileConfig::default(),
            max_lookup_entries: DEFAULT_MAX_LOOKUP_ENTRIES,
        }
    }
}

impl CompileOptions {
    /// The profile the config cascade sets in `expression`, or the default
    /// profile before the cascade loads.
    #[must_use]
    pub fn from_cascade() -> Self {
        Self::default().with_profile(crate::expression::evaluator::get_profile_config().clone())
    }

    /// Compile under `profile`.
    #[must_use]
    pub fn with_profile(mut self, profile: ProfileConfig) -> Self {
        self.profile = profile;
        self
    }

    /// Bound each lookup table at `entries`.
    #[must_use]
    pub fn with_max_lookup_entries(mut self, entries: usize) -> Self {
        self.max_lookup_entries = entries;
        self
    }
}

#[cfg(test)]
mod tests;
