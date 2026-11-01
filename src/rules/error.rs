// Project:   scalo
// File:      src/rules/error.rs
// Purpose:   What a rule set check finds, and the errors rules return
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Findings carry the config location they concern, the rule id when there is
//! one, and for a CEL condition the byte span within its text, so an editor can
//! point at the row and the token.

use serde::Serialize;

use crate::expression::Span;

/// Whether a finding stops the set compiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Severity {
    /// The set does not compile.
    Error,
    /// The set compiles, but a row will never fire as written.
    Warning,
}

/// One finding of a rule set check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct RuleDiagnostic {
    /// Error or warning.
    pub severity: Severity,
    /// Where in the config, e.g. `rules[2].match.value` or `targets[0]`.
    pub location: String,
    /// The id of the row it concerns, when it concerns one.
    pub rule: Option<String>,
    /// What is wrong.
    pub message: String,
    /// Byte range within the text at `location`: the CEL condition's.
    pub span: Option<Span>,
}

impl RuleDiagnostic {
    pub(crate) fn error(location: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            location: location.into(),
            rule: None,
            message: message.into(),
            span: None,
        }
    }

    pub(crate) fn warning(location: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            ..Self::error(location, message)
        }
    }

    pub(crate) fn for_rule(mut self, id: &str) -> Self {
        if !id.is_empty() {
            self.rule = Some(id.to_string());
        }
        self
    }

    pub(crate) fn at(mut self, span: Option<Span>) -> Self {
        self.span = span;
        self
    }

    /// True for a finding that stops the set compiling.
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }
}

impl std::fmt::Display for RuleDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let level = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        write!(f, "{level}: {}", self.location)?;
        if let Some(rule) = &self.rule {
            write!(f, " (rule {rule})")?;
        }
        write!(f, ": {}", self.message)?;
        if let Some(span) = self.span {
            write!(f, " at {span}")?;
        }
        Ok(())
    }
}

/// Why a rule set did not compile or load.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RulesError {
    /// The config has errors. Every finding is listed, warnings included.
    #[error("the rule set is invalid: {}", summary(.0))]
    Invalid(Vec<RuleDiagnostic>),
    /// The file holding the rule set could not be read or parsed.
    #[error("cannot load the rule set from {origin}: {message}")]
    Load {
        /// The file, and the section within it.
        origin: String,
        /// Why.
        message: String,
    },
    /// The `expression` config section does not parse.
    #[error(transparent)]
    Config(#[from] crate::cascade::CascadeError),
}

impl RulesError {
    /// Every finding behind an [`RulesError::Invalid`], empty for a load error.
    #[must_use]
    pub fn diagnostics(&self) -> &[RuleDiagnostic] {
        match self {
            Self::Invalid(found) => found,
            Self::Load { .. } | Self::Config(_) => &[],
        }
    }
}

fn summary(found: &[RuleDiagnostic]) -> String {
    found
        .iter()
        .filter(|d| d.is_error())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_diagnostic_names_its_location_rule_and_span() {
        let d = RuleDiagnostic::error("rules[1].when", "bad call")
            .for_rule("r2")
            .at(Some(Span::new(3, 7)));
        assert_eq!(
            d.to_string(),
            "error: rules[1].when (rule r2): bad call at 3..7"
        );
    }

    #[test]
    fn the_error_summary_lists_errors_only() {
        let err = RulesError::Invalid(vec![
            RuleDiagnostic::warning("rules[0]", "never fires"),
            RuleDiagnostic::error("targets", "is empty"),
        ]);
        assert_eq!(
            err.to_string(),
            "the rule set is invalid: error: targets: is empty"
        );
        assert_eq!(err.diagnostics().len(), 2);
    }
}
