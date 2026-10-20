// Project:   scalo
// File:      src/expression/profile.rs
// Purpose:   Expression profile -- allowed/restricted CEL functions
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Expression profile -- allowed and restricted CEL functions.
//!
//! The profile restricts CEL to a high-performance subset suitable
//! for per-record evaluation at ingest/query time. Functions with
//! unbounded or unpredictable cost are blocked by default but can be
//! unlocked per-category via [`ProfileConfig`].

use super::diagnostic::{Diagnostic, Span};

/// CEL functions allowed unconditionally in the profile.
///
/// Enforced as an ALLOWLIST: a function in neither this slice nor a restricted
/// category is rejected. Listing a function here is what permits it, so anything
/// the profile should accept has to appear -- the cost of an omission is a valid
/// expression refused. Kept in step with scalo-py's
/// `expression/profile.py::ALLOWED_FUNCTIONS`, except `matches`, which is
/// restricted here and unlocked via [`ProfileConfig::allow_regex`].
pub const ALLOWED_FUNCTIONS: &[&str] = &[
    // String operations (SIMD-friendly, bounded cost)
    "contains",
    "startsWith",
    "endsWith",
    // Collection
    "size",
    // Existence
    "has",
    // Type casts
    "int",
    "uint",
    "double",
    "string",
    "bool",
    "bytes",
    // Type introspection. Constant-time, and `dyn()` only relaxes static
    // type-checking; neither iterates.
    "type",
    "dyn",
    // Timestamp and duration accessors: cheap field reads on a value that is
    // already a timestamp. `timestamp()` and `duration()` are restricted, so
    // these are only reachable when the datastore supplies a typed value.
    "getDate",
    "getDayOfMonth",
    "getDayOfWeek",
    "getDayOfYear",
    "getFullYear",
    "getHours",
    "getMilliseconds",
    "getMinutes",
    "getMonth",
    "getSeconds",
];

/// Restricted function categories -- blocked by default, opt-in via config.
///
/// Each category has a reason for restriction and a config flag to unlock.
pub const RESTRICTED_REGEX: &[&str] = &["matches"];
pub const RESTRICTED_ITERATION: &[&str] = &["map", "filter", "exists", "all", "exists_one"];
pub const RESTRICTED_TIME: &[&str] = &["timestamp", "duration"];

/// All restricted functions (union of all categories).
pub const DISALLOWED_FUNCTIONS: &[&str] = &[
    "matches",
    "map",
    "filter",
    "exists",
    "all",
    "exists_one",
    "timestamp",
    "duration",
];

/// Configuration for the expression profile.
///
/// Each flag unlocks a category of restricted functions. All default
/// to `false` (blocked). Set explicitly in application config to opt in.
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct ProfileConfig {
    /// Allow `matches()` (regex). Unbounded cost per record -- use only
    /// when `contains()`/`startsWith()`/`endsWith()` are insufficient.
    pub allow_regex: bool,
    /// Allow `map()`, `filter()`, `exists()`, `all()`, `exists_one()`.
    /// O(n) per collection element -- cost proportional to data size.
    pub allow_iteration: bool,
    /// Allow `timestamp()`, `duration()`. Excluded because ClickHouse
    /// handles time natively -- rarely needed in CEL expressions.
    pub allow_time: bool,
}

impl ProfileConfig {
    /// Returns the set of functions that are currently blocked.
    #[must_use]
    pub fn blocked_functions(&self) -> Vec<&'static str> {
        let mut blocked = Vec::new();
        if !self.allow_regex {
            blocked.extend_from_slice(RESTRICTED_REGEX);
        }
        if !self.allow_iteration {
            blocked.extend_from_slice(RESTRICTED_ITERATION);
        }
        if !self.allow_time {
            blocked.extend_from_slice(RESTRICTED_TIME);
        }
        blocked
    }
}

/// CEL keywords and built-in names that look like function calls but
/// should be skipped during profile scanning.
const SKIP_NAMES: &[&str] = &[
    "true", "false", "null", "in", "has", "int", "uint", "double", "string", "bool",
];

/// Bytes between a function name and its `(` that still leave it a call.
const CALL_GAP: &[u8] = b" \t\r\n\x0c\x0b";

/// Whole names that open a string or bytes literal when a quote follows directly.
const LITERAL_PREFIXES: &[&[u8]] = &[b"r", b"R", b"b", b"B", b"br", b"bR", b"Br", b"BR"];

/// Bytes cel's lexer accepts inside a backtick-quoted identifier.
const ESCAPED_IDENT_BYTES: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-/ ";

/// Scan an expression for restricted function calls using default config.
///
/// Returns a list of error strings (empty if all function calls are
/// within the profile). Equivalent to `check_profile_with_config`
/// with [`ProfileConfig::default()`] (all restrictions active).
#[must_use]
pub fn check_profile(expr: &str) -> Vec<String> {
    check_profile_with_config(expr, &ProfileConfig::default())
}

/// Scan an expression for restricted function calls.
///
/// The scanner reads string and bytes literals (quoted, triple-quoted and
/// raw), `//` comments and backtick-quoted identifiers the way cel's lexer
/// does, so a name inside one is never reported, and whitespace or a comment
/// between a name and its `(` does not hide a call.
///
/// Returns a list of error strings (empty if compliant).
#[must_use]
pub fn check_profile_with_config(expr: &str, config: &ProfileConfig) -> Vec<String> {
    scan_profile(expr, config)
        .into_iter()
        .map(|diagnostic| diagnostic.message)
        .collect()
}

/// [`check_profile_with_config`] with the byte span of each offending function
/// name.
#[must_use]
pub fn scan_profile(expr: &str, config: &ProfileConfig) -> Vec<Diagnostic> {
    // No early return on an empty blocked list. `blocked_functions()` is empty
    // when all three categories are unlocked, and returning here skipped the
    // allowlist check too -- so the most permissive config was also the one that
    // validated nothing at all.
    let blocked = config.blocked_functions();

    let mut errors = Vec::new();
    let bytes = expr.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        let byte = bytes[i];
        if is_quote(byte) {
            i = string_end(bytes, i, false);
            continue;
        }
        if bytes[i..].starts_with(b"//") {
            i = gap_end(bytes, i);
            continue;
        }
        if byte == b'`' {
            i = escaped_ident_end(bytes, i);
            continue;
        }
        if !is_ident_start(byte) {
            i += 1;
            continue;
        }

        let start = i;
        while i < len && is_ident_char(bytes[i]) {
            i += 1;
        }
        let word = &bytes[start..i];
        if i < len && is_quote(bytes[i]) && LITERAL_PREFIXES.contains(&word) {
            let raw = word.iter().any(|b| b.eq_ignore_ascii_case(&b'r'));
            i = string_end(bytes, i, raw);
            continue;
        }
        // An identifier is ASCII, so its bounds are char boundaries.
        let name = &expr[start..i];

        let peek = gap_end(bytes, i);
        if peek < len && bytes[peek] == b'(' {
            if SKIP_NAMES.contains(&name) {
                continue;
            }

            let span = Some(Span::new(start, i));
            if blocked.contains(&name) {
                let reason = restriction_reason(name);
                errors.push(Diagnostic::new(
                    format!(
                        "Function '{name}()' is not allowed in the expression profile. {reason}"
                    ),
                    span,
                ));
            } else if !ALLOWED_FUNCTIONS.contains(&name) && !DISALLOWED_FUNCTIONS.contains(&name) {
                // In neither list. A restricted-but-unlocked function IS in
                // DISALLOWED_FUNCTIONS, so it falls through here and passes.
                errors.push(Diagnostic::new(
                    format!(
                        "Function '{name}()' is not in the expression profile. Check \
                         the spelling, or add it to ALLOWED_FUNCTIONS if it belongs \
                         in the profile."
                    ),
                    span,
                ));
            }
        }
    }

    errors
}

/// The index after the string literal whose opening quote is at `quote_at`.
///
/// Three quotes open a literal that runs to the next three, a raw literal has
/// no escapes, and a one-line literal ends at a CR or LF, which cel refuses.
fn string_end(bytes: &[u8], quote_at: usize, raw: bool) -> usize {
    let quote = bytes[quote_at];
    let step = |b: u8| if !raw && b == b'\\' { 2 } else { 1 };
    let triple = [quote; 3];
    if bytes[quote_at..].starts_with(&triple) {
        let mut i = quote_at + 3;
        while i < bytes.len() {
            if bytes[i..].starts_with(&triple) {
                return i + 3;
            }
            i += step(bytes[i]);
        }
        // With no closing triple, cel reads an empty literal and opens a new one
        // at the third quote.
        return quote_at + 2;
    }
    let mut i = quote_at + 1;
    while i < bytes.len() {
        let b = bytes[i];
        if b == quote {
            return i + 1;
        }
        if b == b'\n' || b == b'\r' {
            return i;
        }
        i += step(b);
    }
    bytes.len()
}

/// The index after the whitespace and `//` comments starting at `i`.
fn gap_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        if CALL_GAP.contains(&bytes[i]) {
            i += 1;
        } else if bytes[i..].starts_with(b"//") {
            i = bytes[i..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(bytes.len(), |n| i + n);
        } else {
            break;
        }
    }
    i
}

/// The index after the backtick-quoted identifier at `start`, or after the
/// backtick alone when none closes.
fn escaped_ident_end(bytes: &[u8], start: usize) -> usize {
    let body = bytes[start + 1..]
        .iter()
        .take_while(|b| ESCAPED_IDENT_BYTES.contains(b))
        .count();
    let close = start + 1 + body;
    if body > 0 && bytes.get(close) == Some(&b'`') {
        close + 1
    } else {
        start + 1
    }
}

fn is_quote(b: u8) -> bool {
    b == b'"' || b == b'\''
}

fn restriction_reason(name: &str) -> &'static str {
    match name {
        "matches" => {
            "Regex has unbounded cost per record. Use contains()/startsWith()/endsWith() instead, or set allow_regex: true in expression config."
        }
        "map" | "filter" | "exists" | "all" | "exists_one" => {
            "Per-element iteration has O(n) cost proportional to collection size. Set allow_iteration: true in expression config to permit."
        }
        "timestamp" | "duration" => {
            "Time functions excluded -- ClickHouse handles time natively. Set allow_time: true in expression config to permit."
        }
        _ => "Restricted by the expression profile.",
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Default config (all restricted) -------------------------

    #[test]
    fn allowed_function_passes() {
        assert_eq!(
            check_profile(r#"msg.contains("error")"#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn starts_with_passes() {
        assert_eq!(
            check_profile(r#"path.startsWith("/api/")"#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn ends_with_passes() {
        assert_eq!(
            check_profile(r#"file.endsWith(".log")"#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn matches_blocked_by_default() {
        let errors = check_profile(r#"name.matches("^web-[0-9]+$")"#);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("matches()"));
        assert!(errors[0].contains("allow_regex"));
    }

    /// A function in neither the allowlist nor a restricted category must be
    /// rejected. The scan only consulted the blocked list, so a typo passed the
    /// profile, and the CEL crate resolves functions at evaluation -- the caller
    /// was told a misspelt expression was valid.
    #[test]
    fn unknown_function_rejected() {
        for expr in [
            "nosuchfunc(x)",
            r#"severity == "critical" && bogus(z)"#,
            "startWith(host)",
            "sizeof(tags) > 3",
        ] {
            let errors = check_profile(expr);
            assert!(
                !errors.is_empty(),
                "{expr} passed the profile despite an unknown function"
            );
        }
    }

    /// Unlocking every restricted category must permit those categories, not
    /// switch the scan off. `blocked_functions()` returns empty when all three
    /// flags are set, and the early return then skipped the allowlist check too,
    /// so the most permissive config was also the one that validated nothing.
    #[test]
    fn all_categories_unlocked_still_rejects_an_unknown_function() {
        let config = ProfileConfig {
            allow_regex: true,
            allow_iteration: true,
            allow_time: true,
        };
        assert!(
            check_profile_with_config(r#"msg.contains("x")"#, &config).is_empty(),
            "an allowed function must still pass with every category unlocked"
        );
        assert!(
            !check_profile_with_config("nosuchfunc(x)", &config).is_empty(),
            "unlocking every category disabled the profile scan entirely"
        );
    }

    /// The allowlist must not refuse legitimate profile expressions. An omission
    /// here rejects a valid operator filter, which is the cost of enforcing it.
    #[test]
    fn profile_functions_still_pass() {
        for expr in [
            r#"has(user.id) && host.startsWith("web-")"#,
            "size(tags) > 3",
            r#"int(count) > 100 && message.contains("error")"#,
            r#"double(score) >= 0.5 && string(id).endsWith("-x")"#,
            "bytes(payload).size() > 0",
            "type(value) == string",
            r#"dyn(meta).contains("k")"#,
            "created.getFullYear() == 2026",
        ] {
            let errors = check_profile(expr);
            assert!(errors.is_empty(), "{expr} was rejected: {errors:?}");
        }
    }

    #[test]
    fn disallowed_map_rejected() {
        let errors = check_profile("[1,2,3].map(x, x * 2)");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("map()"));
    }

    #[test]
    fn disallowed_filter_rejected() {
        let errors = check_profile("[1,2,3].filter(x, x > 1)");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("filter()"));
    }

    #[test]
    fn disallowed_timestamp_rejected() {
        let errors = check_profile(r#"timestamp("2024-01-01T00:00:00Z")"#);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("timestamp()"));
    }

    #[test]
    fn disallowed_duration_rejected() {
        let errors = check_profile(r#"duration("1h")"#);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("duration()"));
    }

    #[test]
    fn keywords_skipped() {
        assert_eq!(
            check_profile("has(user.name)"),
            [] as [std::string::String; 0]
        );
        assert_eq!(check_profile("int(x) > 10"), [] as [std::string::String; 0]);
        assert_eq!(check_profile("bool(y)"), [] as [std::string::String; 0]);
    }

    #[test]
    fn plain_comparison_passes() {
        assert_eq!(
            check_profile(r#"severity == "critical""#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn compound_expression_passes() {
        assert_eq!(
            check_profile(r#"severity == "critical" && amount > 10000"#),
            [] as [std::string::String; 0]
        );
    }

    // -- String literal false-positive prevention ----------------

    #[test]
    fn function_name_inside_string_not_flagged() {
        // "filter" appears inside a string literal, not as a function call
        assert_eq!(
            check_profile(r#"msg.contains("filter")"#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn function_name_inside_string_with_parens_not_flagged() {
        // "map(" appears inside a string -- should not be flagged
        assert_eq!(
            check_profile(r#"msg.contains("map(x)")"#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn matches_inside_string_not_flagged() {
        assert_eq!(
            check_profile(r#"msg.contains("matches")"#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn timestamp_inside_string_not_flagged() {
        assert_eq!(
            check_profile(r#"label == "timestamp""#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn escaped_quote_inside_string_handled() {
        // String with escaped quote: "filter\"(" -- scanner must not exit early
        assert_eq!(
            check_profile(r#"msg.contains("filter\"(")"#),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn single_quoted_string_handled() {
        assert_eq!(
            check_profile("msg.contains('filter')"),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn real_call_after_string_still_caught() {
        // String contains "ok" but then a real map() call follows
        let errors = check_profile(r#""ok" + items.map(x, x)"#);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("map()"));
    }

    // -- Config overrides ----------------------------------------

    #[test]
    fn matches_allowed_with_regex_config() {
        let config = ProfileConfig {
            allow_regex: true,
            ..Default::default()
        };
        assert_eq!(
            check_profile_with_config(r#"name.matches("^web-[0-9]+$")"#, &config),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn map_still_blocked_with_regex_config() {
        let config = ProfileConfig {
            allow_regex: true,
            ..Default::default()
        };
        let errors = check_profile_with_config("[1,2].map(x, x)", &config);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("map()"));
    }

    #[test]
    fn iteration_allowed_with_config() {
        let config = ProfileConfig {
            allow_iteration: true,
            ..Default::default()
        };
        assert_eq!(
            check_profile_with_config("[1,2].map(x, x * 2)", &config),
            [] as [std::string::String; 0]
        );
        assert_eq!(
            check_profile_with_config("[1,2].filter(x, x > 1)", &config),
            [] as [std::string::String; 0]
        );
        assert_eq!(
            check_profile_with_config("[1,2].exists(x, x > 1)", &config),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn time_allowed_with_config() {
        let config = ProfileConfig {
            allow_time: true,
            ..Default::default()
        };
        assert_eq!(
            check_profile_with_config(r#"timestamp("2024-01-01T00:00:00Z")"#, &config),
            [] as [std::string::String; 0]
        );
        assert_eq!(
            check_profile_with_config(r#"duration("1h")"#, &config),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn all_restrictions_lifted() {
        let config = ProfileConfig {
            allow_regex: true,
            allow_iteration: true,
            allow_time: true,
        };
        assert_eq!(config.blocked_functions(), [] as [&str; 0]);
        assert_eq!(
            check_profile_with_config(r#"name.matches("x") && [1].map(x, x)"#, &config),
            [] as [std::string::String; 0]
        );
    }

    // -- Edge cases ----------------------------------------------

    #[test]
    fn identifier_not_followed_by_paren_is_fine() {
        // "filter" as a variable name, not a function call
        assert_eq!(check_profile("filter > 10"), [] as [std::string::String; 0]);
    }

    #[test]
    fn identifier_with_space_before_paren() {
        let errors = check_profile("[1,2].map (x, x)");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("map()"));
    }

    #[test]
    fn empty_expression() {
        assert_eq!(check_profile(""), [] as [std::string::String; 0]);
    }

    #[test]
    fn whitespace_only() {
        assert_eq!(check_profile("   "), [] as [std::string::String; 0]);
    }

    #[test]
    fn multiple_violations_reported() {
        let errors = check_profile("[1].map(x, x).filter(y, y > 0)");
        assert_eq!(errors.len(), 2);
    }

    // -- Spans ----------------------------------------------------

    /// Each finding carries the byte range of the function name, so an editor
    /// can underline the call rather than the whole expression.
    #[test]
    fn scan_spans_cover_the_function_name() {
        let expr = r#"host.startsWith("w") && name.matches("^x") || nosuch(y)"#;
        let found = scan_profile(expr, &ProfileConfig::default());
        let named: Vec<&str> = found
            .iter()
            .map(|d| {
                let span = d.span.expect("a profile finding has a span");
                &expr[span.start..span.end]
            })
            .collect();
        assert_eq!(named, ["matches", "nosuch"]);
    }

    /// The string-only API reports exactly what the span scan finds.
    #[test]
    fn check_profile_is_the_span_scan_without_spans() {
        let expr = "[1].map(x, x) && bogus(z)";
        let config = ProfileConfig::default();
        let messages: Vec<String> = scan_profile(expr, &config)
            .into_iter()
            .map(|d| d.message)
            .collect();
        assert_eq!(check_profile_with_config(expr, &config), messages);
    }

    /// A name inside a string literal is skipped, so a later call's span is
    /// still the call's own bytes.
    #[test]
    fn a_span_after_a_string_literal_is_the_calls_own() {
        let expr = r#"msg.contains("map(") && x.map(a, a)"#;
        let found = scan_profile(expr, &ProfileConfig::default());
        assert_eq!(found.len(), 1);
        let span = found[0].span.expect("span");
        assert_eq!(&expr[span.start..span.end], "map");
        assert_eq!(span.start, expr.rfind("map").expect("second map"));
    }

    // -- Lexer forms ----------------------------------------------

    /// One restricted call per category, as a receiver, a name and its arguments.
    const RESTRICTED_CALLS: &[(&str, &str, &str)] = &[
        ("x.", "matches", r#""a""#),
        ("[1].", "map", "v, v"),
        ("[1].", "filter", "v, v > 0"),
        ("[1].", "exists", "v, v > 0"),
        ("[1].", "all", "v, v > 0"),
        ("[1].", "exists_one", "v, v > 0"),
        ("", "timestamp", r#""2024-01-01T00:00:00Z""#),
        ("", "duration", r#""1h""#),
    ];

    /// Literal and comment forms that hide `{call}` from a scanner reading them
    /// differently from cel's lexer.
    const LITERAL_FORMS: &[(&str, &str)] = &[
        ("raw r double", r#"r"\" == "x" || {call}"#),
        ("raw R double", r#"R"\" == "x" || {call}"#),
        ("raw r single", r"r'\' == 'x' || {call}"),
        ("raw R single", r"R'\' == 'x' || {call}"),
        ("raw triple double", r#"r"""\""" == "x" || {call}"#),
        ("raw triple single", r"r'''\''' == 'x' || {call}"),
        ("triple double", r#""""a"b""" == "x" || {call}"#),
        ("triple single", "'''a'b''' == 'x' || {call}"),
        ("raw bytes br", r#"br"\" == b"x" || {call}"#),
        ("raw bytes bR", r#"bR"\" == b"x" || {call}"#),
        ("raw bytes Br", r#"Br"\" == b"x" || {call}"#),
        ("raw bytes BR single", r"BR'\' == b'x' || {call}"),
        ("bytes triple", r#"b"""a"b""" == b"x" || {call}"#),
        ("comment holding a quote", "1 == 1 // \"\n || {call}"),
    ];

    /// Gaps cel accepts between a function name and its `(`.
    const CALL_GAPS: &[(&str, &str)] = &[
        ("comment", "//\n"),
        ("spaced comment", " // note\n"),
        ("comment line", "\n// note\n "),
        ("tab", "\t"),
        ("carriage return", "\r"),
        ("newline", "\n"),
        ("form feed", "\x0c"),
        ("mixed", " \t\n "),
    ];

    fn call(receiver: &str, name: &str, gap: &str, args: &str) -> String {
        format!("{receiver}{name}{gap}({args})")
    }

    /// Every form is valid CEL that runs the call, so the profile has to refuse it.
    fn assert_refused(form: &str, expr: &str, name: &str) {
        if let Err(e) = cel::Program::compile(expr) {
            panic!("{form}: {expr:?} is not valid CEL, so it proves nothing: {e}");
        }
        let errors = check_profile(expr);
        assert!(
            errors.iter().any(|e| e.contains(&format!("'{name}()'"))),
            "{form}: {expr:?} hid {name}() from the profile: {errors:?}"
        );
    }

    #[test]
    fn no_literal_or_comment_form_hides_a_restricted_call() {
        for &(receiver, name, args) in RESTRICTED_CALLS {
            for &(form, template) in LITERAL_FORMS {
                let expr = template.replace("{call}", &call(receiver, name, "", args));
                assert_refused(form, &expr, name);
            }
        }
    }

    #[test]
    fn no_gap_before_the_bracket_hides_a_restricted_call() {
        for &(receiver, name, args) in RESTRICTED_CALLS {
            for &(form, gap) in CALL_GAPS {
                assert_refused(form, &call(receiver, name, gap, args), name);
            }
        }
    }

    /// cel's lexer refuses a vertical tab, but the scan treats it as a gap so a
    /// lexer that accepted one could not run the call.
    #[test]
    fn a_vertical_tab_before_the_bracket_is_still_a_call() {
        let errors = check_profile("x.matches\x0b(\"a\")");
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("'matches()'"));
    }

    /// A call inside a literal or a comment is data, so it is not reported.
    #[test]
    fn a_call_inside_a_literal_or_comment_is_not_scanned() {
        for expr in [
            r#"r"map(" == "x""#,
            r#""""a"b map( c""" == "x""#,
            "'''x.matches('a')''' == 'x'",
            r#"br"duration(" == b"x""#,
            "R'''filter(''' == 'x'",
            "x == 1 // map(v, v)",
            "x == 1 // \"\n",
            "x == 1 // matches(\r || x == 2",
        ] {
            assert!(
                cel::Program::compile(expr).is_ok(),
                "{expr:?} is not valid CEL"
            );
            assert_eq!(check_profile(expr), [] as [String; 0], "{expr:?}");
        }
    }

    /// A line break ends a one-line literal, so a call on the next line is seen.
    #[test]
    fn a_line_break_ends_a_one_line_literal() {
        for expr in ["\"abc\nx.matches(\"a\")", "r'abc\rx.matches('a')"] {
            let errors = check_profile(expr);
            assert_eq!(errors.len(), 1, "{expr:?}: {errors:?}");
            assert!(errors[0].contains("'matches()'"));
        }
    }

    /// With no closing triple quote, cel reads an empty literal and opens a new
    /// one at the third quote, so the call after that literal is still a call.
    #[test]
    fn an_unclosed_triple_quote_is_an_empty_literal_then_a_new_one() {
        let expr = r#""""a" || x.matches("a")"#;
        assert!(cel::Program::compile(expr).is_err(), "{expr:?} parsed");
        let found = scan_profile(expr, &ProfileConfig::default());
        let named: Vec<&str> = found
            .iter()
            .filter_map(|d| d.span.map(|s| &expr[s.start..s.end]))
            .collect();
        assert_eq!(named, ["matches"]);
    }

    /// A prefix opens a literal only as a whole name of r, b or both, directly
    /// before a quote.
    #[test]
    fn a_prefix_opens_a_literal_only_as_a_whole_name() {
        // `rb` and `xr` are names, so the backslash escapes the quote and the
        // plain literal runs on, which cel refuses as a name beside a string.
        for expr in [
            r#"rb"\" == "x" || x.matches("a")"#,
            r#"xr"\" == "x" || x.matches("a")"#,
        ] {
            assert_eq!(check_profile(expr), [] as [String; 0], "{expr:?}");
            assert!(cel::Program::compile(expr).is_err(), "{expr:?} parsed");
        }
        // A space between the prefix and the quote leaves a name and a plain literal.
        let expr = r#"r "\" || x.matches("a")"#;
        assert_eq!(check_profile(expr), [] as [String; 0]);
        assert!(cel::Program::compile(expr).is_err(), "{expr:?} parsed");
    }

    /// A backtick-quoted name is skipped whole, so `//` inside it is not a comment.
    #[test]
    fn a_backtick_name_is_not_a_comment() {
        let errors = check_profile(r#"x.`a//b` == 1 || x.matches("a")"#);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("'matches()'"));
        // A lone backtick is one byte, so the scan carries on after it.
        let errors = check_profile(r#"` x.matches("a")"#);
        assert_eq!(errors.len(), 1, "{errors:?}");
    }

    /// The span still covers only the function name, whatever the gap.
    #[test]
    fn a_span_across_a_gap_covers_only_the_name() {
        let expr = "x.map // note\n\t (a, a)";
        let found = scan_profile(expr, &ProfileConfig::default());
        assert_eq!(found.len(), 1, "{found:?}");
        let span = found[0].span.expect("span");
        assert_eq!(&expr[span.start..span.end], "map");
    }

    // -- Differential against cel's parser -------------------------

    /// Text mixed around a call, covering every literal, comment and gap form.
    const PIECES: &[&str] = &[
        "\"", "'", "\"\"\"", "'''", "r", "R", "b", "B", "br", "bR", "Br", "BR", "rb", "\\", "//",
        "\n", "\r", "\t", "\x0c", " ", "`", "a", "x", "1", "==", "||", "(", ")", ".", ",", "[",
        "]",
    ];

    /// Calls the default profile refuses, some with a gap before the bracket.
    const CALLS: &[&str] = &[
        "x.matches(\"a\")",
        "x.matches//\n(\"a\")",
        "timestamp(\"2024-01-01T00:00:00Z\")",
        "duration // c\n(\"1h\")",
        "[1].map(v, v)",
        "[1].filter\n(v, v > 0)",
        "[1].exists_one\t(v, v > 0)",
        "[1].all\r\n(v, v > 0)",
        "[1].exists(v, v > 0)",
        "nosuch(1)",
    ];

    const SHAPES: &[&str] = &[
        "{h} == \"x\" || {c}",
        "{h} || {c}",
        "{c} || {h}",
        "[{h}] == [] || {c}",
        "{h}{c}",
    ];

    /// Macros cel expands into a comprehension rather than a call.
    const ITERATION_MACROS: &[&str] =
        &["map", "filter", "exists", "all", "exists_one", "existsOne"];

    /// SplitMix64, so a failing mix reproduces from its seed.
    struct SplitMix(u64);

    impl SplitMix {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let n = u64::try_from(n).expect("a slice length fits in u64");
            usize::try_from(z % n).expect("below a slice length")
        }

        fn pick<'a>(&mut self, from: &[&'a str]) -> &'a str {
            from[self.below(from.len())]
        }
    }

    /// Whether any macro in `e` expanded to a comprehension.
    fn iterates(e: &cel::IdedExpr) -> bool {
        use cel::common::ast::{EntryExpr, Expr, IdedEntryExpr};
        fn entry(e: &IdedEntryExpr) -> bool {
            match &e.expr {
                EntryExpr::StructField(f) => iterates(&f.value),
                EntryExpr::MapEntry(m) => iterates(&m.key) || iterates(&m.value),
            }
        }
        match &e.expr {
            Expr::Comprehension(_) => true,
            Expr::Call(c) => {
                c.target.as_deref().is_some_and(iterates) || c.args.iter().any(iterates)
            }
            Expr::List(l) => l.elements.iter().any(iterates),
            Expr::Map(m) => m.entries.iter().any(entry),
            Expr::Struct(s) => s.entries.iter().any(entry),
            Expr::Select(s) => iterates(&s.operand),
            Expr::Ident(_) | Expr::Literal(_) | Expr::Unspecified => false,
        }
    }

    /// Mix `cases` expressions from `seed` and, for each one cel parses, require
    /// the scan to report exactly the calls cel's parser read that the default
    /// profile refuses. Returns how many cel parsed.
    fn differential(cases: usize, seed: u64) -> usize {
        let mut rng = SplitMix(seed);
        let mut parsed = 0;
        for _ in 0..cases {
            let head: String = (0..=rng.below(10)).map(|_| rng.pick(PIECES)).collect();
            let shape = rng.pick(SHAPES);
            let expr = shape.replace("{h}", &head).replace("{c}", rng.pick(CALLS));
            let Ok(program) = cel::Program::compile(&expr) else {
                continue;
            };
            parsed += 1;
            let references = program.references();
            // Operators are named with punctuation, and a leading-dot call keeps its dot.
            let called: Vec<&str> = references
                .functions()
                .into_iter()
                .map(|f| f.trim_start_matches('.'))
                .filter(|f| f.bytes().all(is_ident_char) && !ALLOWED_FUNCTIONS.contains(f))
                .collect();
            let iterated = iterates(program.expression());
            let flagged: Vec<&str> = scan_profile(&expr, &ProfileConfig::default())
                .iter()
                .filter_map(|d| d.span.map(|s| &expr[s.start..s.end]))
                .collect();
            for name in &called {
                assert!(
                    flagged.contains(name),
                    "{expr:?} hid {name}() from the scan"
                );
            }
            if iterated {
                assert!(
                    flagged.iter().any(|f| ITERATION_MACROS.contains(f)),
                    "{expr:?} iterates, and the scan reported {flagged:?}"
                );
            }
            for name in &flagged {
                assert!(
                    called.contains(name) || (iterated && ITERATION_MACROS.contains(name)),
                    "{expr:?}: the scan reported {name}(), which cel does not call"
                );
            }
        }
        parsed
    }

    #[test]
    fn the_scan_agrees_with_cels_parser_on_mixed_literals() {
        let parsed = differential(20_000, 20_261_021);
        assert!(parsed > 1_000, "only {parsed} mixes parsed");
    }

    #[test]
    #[ignore = "400K-case differential, slow in a debug build -- run: cargo nextest run --release --features expression --run-ignored all --ignore-default-filter -E 'test(differential)'"]
    fn the_scan_agrees_with_cels_parser_over_400k_mixes_differential() {
        let parsed = differential(400_000, 7);
        assert!(parsed > 20_000, "only {parsed} mixes parsed");
    }
}
