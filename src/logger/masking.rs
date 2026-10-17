// Project:   scalo
// File:      src/logger/masking.rs
// Purpose:   Sensitive data masking for log output
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sensitive data masking for tracing log output.
//!
//! Provides a [`MaskingWriter`] that intercepts formatted log output and
//! redacts sensitive field values before writing to the underlying destination.

use std::borrow::Cow;
use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use super::credential::{CredentialMatcher, SENSITIVE_FIELDS, mask_text};

/// Redacted value placeholder.
pub const REDACTED: &str = "[REDACTED]";

/// Default list of sensitive field names to mask.
///
/// A field name holding any of these anywhere in it, in any case and with any
/// separators, is masked; see [`is_credential_name`](super::is_credential_name).
#[must_use]
pub fn default_sensitive_fields() -> Vec<String> {
    SENSITIVE_FIELDS.iter().map(ToString::to_string).collect()
}

/// Configuration for sensitive field detection.
///
/// Holds the set of field name patterns considered sensitive. Used both as a
/// standalone detector and as configuration for the masking writer factory.
#[derive(Debug, Clone)]
pub struct MaskingLayer {
    sensitive_fields: HashSet<String>,
    matcher: CredentialMatcher,
}

impl MaskingLayer {
    /// Create a new masking layer with default sensitive fields.
    #[must_use]
    pub fn new() -> Self {
        Self::with_fields(default_sensitive_fields())
    }

    /// Create a masking layer with custom sensitive fields.
    #[must_use]
    pub fn with_fields(fields: Vec<String>) -> Self {
        let sensitive_fields: HashSet<String> =
            fields.into_iter().map(|s| s.to_lowercase()).collect();
        let matcher = CredentialMatcher::new(&sensitive_fields);
        Self {
            sensitive_fields,
            matcher,
        }
    }

    /// Add additional sensitive fields.
    #[must_use]
    pub fn add_fields(mut self, fields: Vec<String>) -> Self {
        for field in fields {
            self.sensitive_fields.insert(field.to_lowercase());
        }
        self.matcher = CredentialMatcher::new(&self.sensitive_fields);
        self
    }

    /// Check if a field name should be masked.
    ///
    /// A name is masked when one of this layer's fields sits anywhere in it,
    /// read as written and word by word, so `secretAccessKey` matches
    /// `secret_access_key`.
    #[must_use]
    pub fn should_mask(&self, field_name: &str) -> bool {
        self.matcher.matches(field_name, "")
    }
}

impl Default for MaskingLayer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Writer-based masking
// ---------------------------------------------------------------------------

/// Create a masking writer factory for use with tracing-subscriber's `with_writer`.
///
/// Returns a closure that produces [`MaskingWriter`] instances wrapping stderr.
/// When the sensitive fields set is empty and no service fields are set, the
/// writer passes through without buffering or redaction.
pub fn make_masking_writer(
    sensitive_fields: HashSet<String>,
    is_json: bool,
    service_name: Option<String>,
    service_version: Option<String>,
) -> impl Fn() -> MaskingWriter<io::Stderr> + Send + Sync {
    let matcher = Arc::new(CredentialMatcher::new(&sensitive_fields));
    let name = service_name.map(Arc::from);
    let version = service_version.map(Arc::from);
    move || MaskingWriter {
        inner: io::stderr(),
        buffer: Vec::with_capacity(512),
        matcher: Arc::clone(&matcher),
        is_json,
        service_name: name.clone(),
        service_version: version.clone(),
    }
}

/// A writer that redacts sensitive field values from formatted log output.
///
/// Buffers each log line (tracing-subscriber writes complete lines via
/// `write_all`), applies field-level redaction, then flushes to the inner
/// writer. When the sensitive fields set is empty and no service fields are
/// set, writes pass through directly with no buffering overhead.
///
/// JSON mode masks the value of every key that names a credential at any
/// depth, a bare `key` or `value` judged with its parent's name, the second
/// item of a two-item array inside an array whose first item names one, and
/// every string with the text rules. Text mode masks the line with the text
/// rules: URL passwords, bearer tokens, `key: value` and `key=value`.
pub struct MaskingWriter<W: io::Write> {
    inner: W,
    buffer: Vec<u8>,
    matcher: Arc<CredentialMatcher>,
    is_json: bool,
    /// Service name injected into JSON log output (JSON mode only).
    service_name: Option<Arc<str>>,
    /// Service version injected into JSON log output (JSON mode only).
    service_version: Option<Arc<str>>,
}

impl<W: io::Write> MaskingWriter<W> {
    /// Create a new masking writer wrapping the given writer.
    ///
    /// When `sensitive_fields` is empty, writes pass through with no overhead.
    /// Set `is_json` to `true` for JSON-format redaction, `false` for text.
    #[must_use]
    pub fn new(inner: W, sensitive_fields: Arc<HashSet<String>>, is_json: bool) -> Self {
        Self {
            inner,
            buffer: Vec::with_capacity(512),
            matcher: Arc::new(CredentialMatcher::new(sensitive_fields.iter())),
            is_json,
            service_name: None,
            service_version: None,
        }
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        {
            let line = String::from_utf8_lossy(&self.buffer);
            let redacted: Cow<'_, str> = if self.is_json {
                Cow::Owned(inject_and_redact_json_line(
                    &line,
                    &self.matcher,
                    self.service_name.as_deref(),
                    self.service_version.as_deref(),
                ))
            } else {
                // A coloured `name=value` only reads as a pair with its escapes gone, so a line holding a credential is written without colour.
                let masked = match mask_text(&strip_ansi(&line), &self.matcher, REDACTED) {
                    Cow::Borrowed(_) => None,
                    Cow::Owned(masked) => Some(masked),
                };
                masked.map_or(Cow::Borrowed(&*line), Cow::Owned)
            };
            self.inner.write_all(redacted.as_bytes())?;
        }
        self.buffer.clear();
        Ok(())
    }

    /// Returns `true` if this writer must buffer output (masking or injection active).
    fn needs_buffering(&self) -> bool {
        !self.matcher.is_empty() || self.service_name.is_some() || self.service_version.is_some()
    }
}

impl<W: io::Write> io::Write for MaskingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.needs_buffering() {
            return self.inner.write(buf);
        }
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.needs_buffering() {
            self.flush_buffer()?;
        }
        self.inner.flush()
    }
}

impl<W: io::Write> Drop for MaskingWriter<W> {
    fn drop(&mut self) {
        if self.needs_buffering() {
            let _ = self.flush_buffer();
        }
    }
}

// ---------------------------------------------------------------------------
// Redaction functions
// ---------------------------------------------------------------------------

/// `line` without its ANSI control sequences (`ESC [` ... final byte).
fn strip_ansi(line: &str) -> Cow<'_, str> {
    if !line.contains('\x1b') {
        return Cow::Borrowed(line);
    }
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            chars.next();
            for c in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&c) {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    Cow::Owned(out)
}

/// Inject service fields and redact sensitive fields in a JSON log line.
///
/// Parses the line as JSON, inserts `service` and `version` at the root level
/// (if provided), walks the object tree and replaces values of sensitive keys
/// with `[REDACTED]`, then re-serialises. A line that is not JSON is masked as
/// text.
fn inject_and_redact_json_line(
    line: &str,
    matcher: &CredentialMatcher,
    service_name: Option<&str>,
    service_version: Option<&str>,
) -> String {
    let trimmed = line.trim_end_matches('\n');
    if let Ok(mut value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let serde_json::Value::Object(ref mut map) = value {
            if let Some(name) = service_name {
                map.insert(
                    "service".to_string(),
                    serde_json::Value::String(name.to_string()),
                );
            }
            if let Some(ver) = service_version {
                map.insert(
                    "version".to_string(),
                    serde_json::Value::String(ver.to_string()),
                );
            }

            // Inject K8s context fields (no-op on bare metal -- fields are None)
            let ctx = crate::env::runtime_context();
            if let Some(ref pod) = ctx.pod_name {
                map.insert(
                    "pod_name".to_string(),
                    serde_json::Value::String(pod.clone()),
                );
            }
            if let Some(ref ns) = ctx.namespace {
                map.insert(
                    "namespace".to_string(),
                    serde_json::Value::String(ns.clone()),
                );
            }
            if let Some(ref node) = ctx.node_name {
                map.insert(
                    "node_name".to_string(),
                    serde_json::Value::String(node.clone()),
                );
            }
        }
        if !matcher.is_empty() {
            redact_json_value(&mut value, "", false, matcher);
        }
        let mut result = serde_json::to_string(&value).unwrap_or_else(|_| trimmed.to_string());
        if line.ends_with('\n') {
            result.push('\n');
        }
        result
    } else if matcher.is_empty() {
        line.to_string()
    } else {
        mask_text(line, matcher, REDACTED).into_owned()
    }
}

/// Redact a JSON value held under `parent`.
///
/// A key naming a credential masks its whole value, and a string is masked
/// with the text rules. `entry` is true for an item of an array, where a
/// two-item array whose first item names a credential is a `(name, value)`
/// pair; under a key a two-item array is a list.
fn redact_json_value(
    value: &mut serde_json::Value,
    parent: &str,
    entry: bool,
    matcher: &CredentialMatcher,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                if matcher.matches(key, parent) {
                    *item = serde_json::Value::String(REDACTED.to_string());
                } else {
                    redact_json_value(item, key, false, matcher);
                }
            }
        }
        serde_json::Value::Array(items) => {
            let is_pair = entry
                && items.len() == 2
                && items[0]
                    .as_str()
                    .is_some_and(|name| matcher.matches(name, parent));
            if is_pair {
                redact_json_value(&mut items[0], parent, true, matcher);
                items[1] = serde_json::Value::String(REDACTED.to_string());
                return;
            }
            for item in items {
                redact_json_value(item, parent, true, matcher);
            }
        }
        serde_json::Value::String(text) => {
            if let Cow::Owned(masked) = mask_text(text, matcher, REDACTED) {
                *text = masked;
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

/// Mask sensitive values in a string.
///
/// Masks the value after every `key: value` and `key=value` whose key holds one
/// of `patterns`, every URL password and every bearer token, writing
/// `[REDACTED]` in place. A key is judged as [`MaskingLayer::should_mask`]
/// judges a field name, so `secretAccessKey` matches `secret_access_key`. A
/// value runs to its closing quote, past escaped quotes, or to its next quote
/// when no unescaped one closes it, and a `Name(...)` value such as
/// `Some("...")` goes whole, so a `{:#?}` dump of a config is safe to print. A
/// quoted or `Name(...)` value that never closes is masked to the end of its
/// line, or to the end of the text when its reading may cross lines. After a
/// key naming an authorization header, an unquoted value is the scheme and the
/// credentials after it, so `Authorization: Basic ...` masks whole, and so is
/// one opening with an authentication scheme after any credential key, as in
/// `x-api-key: Token ...`.
///
/// # Examples
///
/// ```
/// use scalo::logger::{default_sensitive_fields, mask_sensitive_string};
///
/// let fields = default_sensitive_fields();
/// let patterns: Vec<&str> = fields.iter().map(String::as_str).collect();
/// let dump = "Config { host: \"db\", db_password: Some(\"hunter2\") }";
/// assert_eq!(
///     mask_sensitive_string(dump, &patterns),
///     "Config { host: \"db\", db_password: [REDACTED] }"
/// );
/// ```
#[must_use]
pub fn mask_sensitive_string(input: &str, patterns: &[&str]) -> String {
    mask_text(input, &CredentialMatcher::new(patterns), REDACTED).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Shared buffer for testing MaskingWriter (survives writer drop)
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn matcher(fields: &[&str]) -> CredentialMatcher {
        CredentialMatcher::new(fields)
    }

    fn writer(
        buf: &Arc<Mutex<Vec<u8>>>,
        fields: &[&str],
        is_json: bool,
    ) -> MaskingWriter<TestWriter> {
        MaskingWriter {
            inner: TestWriter(Arc::clone(buf)),
            buffer: Vec::new(),
            matcher: Arc::new(matcher(fields)),
            is_json,
            service_name: None,
            service_version: None,
        }
    }

    #[test]
    fn test_default_sensitive_fields() {
        let fields = default_sensitive_fields();
        assert!(fields.contains(&"password".to_string()));
        assert!(fields.contains(&"token".to_string()));
        assert!(fields.contains(&"api_key".to_string()));
        assert!(fields.contains(&"secret".to_string()));
    }

    #[test]
    fn test_masking_layer_should_mask() {
        let layer = MaskingLayer::new();

        assert!(layer.should_mask("password"));
        assert!(layer.should_mask("PASSWORD"));
        assert!(layer.should_mask("user_password"));
        assert!(layer.should_mask("api_key"));
        assert!(layer.should_mask("secret_token"));

        assert!(!layer.should_mask("username"));
        assert!(!layer.should_mask("host"));
        assert!(!layer.should_mask("port"));
    }

    #[test]
    fn test_masking_layer_custom_fields() {
        let layer = MaskingLayer::with_fields(vec!["custom_secret".to_string()]);

        assert!(layer.should_mask("custom_secret"));
        assert!(!layer.should_mask("password")); // Not in custom list
    }

    #[test]
    fn test_masking_layer_add_fields() {
        let layer = MaskingLayer::new().add_fields(vec!["my_custom_field".to_string()]);

        assert!(layer.should_mask("my_custom_field"));
        assert!(layer.should_mask("password")); // Still has defaults
    }

    #[test]
    fn an_added_field_is_matched_in_any_spelling() {
        let layer = MaskingLayer::new().add_fields(vec!["employee_ref".to_string()]);

        assert!(layer.should_mask("EmployeeRef"));
        assert!(!MaskingLayer::new().should_mask("EmployeeRef"));
    }

    #[test]
    fn test_mask_sensitive_string() {
        let input = "password=secret123 username=john";
        let result = mask_sensitive_string(input, &["password"]);
        assert!(result.contains("[REDACTED]"));
        assert!(result.contains("username=john"));
    }

    // --- JSON redaction tests ---

    #[test]
    fn test_redact_json_line_sensitive_field() {
        let input =
            "{\"level\":\"INFO\",\"fields\":{\"message\":\"hello\",\"password\":\"secret123\"}}\n";
        let result = inject_and_redact_json_line(input, &matcher(&["password"]), None, None);
        assert!(result.contains("[REDACTED]"));
        assert!(!result.contains("secret123"));
        assert!(result.contains("hello"));
        assert!(result.ends_with('\n'));
    }

    #[test]
    fn test_redact_json_line_nested() {
        let input = r#"{"fields":{"config":{"token":"abc123","host":"localhost"}}}"#;
        let result = inject_and_redact_json_line(input, &matcher(&["token"]), None, None);
        assert!(!result.contains("abc123"));
        assert!(result.contains("localhost"));
    }

    #[test]
    fn test_redact_json_line_preserves_non_sensitive() {
        let input = r#"{"level":"INFO","fields":{"username":"john","host":"db.example.com"}}"#;
        let result = inject_and_redact_json_line(input, &matcher(&["password"]), None, None);
        assert!(result.contains("john"));
        assert!(result.contains("db.example.com"));
    }

    #[test]
    fn test_redact_json_line_invalid_json_passthrough() {
        let input = "this is not json\n";
        let result = inject_and_redact_json_line(input, &matcher(&["password"]), None, None);
        assert_eq!(result, input);
    }

    #[test]
    fn a_line_that_is_not_json_is_masked_as_text() {
        let input = "half a line password=secret123 {\n";
        let result = inject_and_redact_json_line(input, &matcher(&["password"]), None, None);
        assert_eq!(result, "half a line password=[REDACTED] {\n");
    }

    #[test]
    fn a_json_message_is_masked_as_text() {
        let input =
            r#"{"fields":{"message":"connecting to postgres://svc:p@ss/w@db/app as apiKey=abc"}}"#;
        let result =
            inject_and_redact_json_line(input, CredentialMatcher::default_terms(), None, None);
        // Read the message alone: in a pod the line also gains pod_name and namespace.
        let line: serde_json::Value = serde_json::from_str(&result).expect("still JSON");
        assert_eq!(
            line["fields"]["message"],
            "connecting to postgres://svc:[REDACTED]@db/app as apiKey=[REDACTED]"
        );
    }

    #[test]
    fn a_json_line_with_masking_off_keeps_its_strings() {
        let input = r#"{"fields":{"message":"postgres://svc:pw@db/app"}}"#;
        let result = inject_and_redact_json_line(input, &matcher(&[]), Some("svc"), None);
        assert!(result.contains("postgres://svc:pw@db/app"), "{result}");
    }

    /// The structured cases of the shared corpus, walked as a record's fields.
    #[test]
    fn structured_cases_match_the_shared_corpus() {
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/credential_masking_parity.json"
        ))
        .expect("corpus parses");
        let secret = format!("FAKE-{}-{}", "credname", "X".repeat(12));
        let fill = |value: &serde_json::Value| -> serde_json::Value {
            let text = value.to_string().replace("{secret}", &secret);
            serde_json::from_str(&text.replace("{mask}", REDACTED)).expect("filled case parses")
        };
        let cases = corpus["structured_cases"]
            .as_array()
            .expect("structured_cases");
        assert!(cases.len() >= 10, "corpus lost its structured cases");
        for case in cases {
            let mut fields = fill(&case["input"]);
            redact_json_value(&mut fields, "", false, CredentialMatcher::default_terms());
            assert_eq!(fields, fill(&case["expected"]), "case {}", case["id"]);
        }
    }

    // --- Text redaction tests ---

    #[test]
    fn test_redact_text_line_quoted_value() {
        let input = r#"2026-01-01T00:00:00Z  INFO target: hello password="secret123" user="john""#;
        let result = mask_text(input, &matcher(&["password"]), REDACTED);
        assert!(!result.contains("secret123"));
        assert!(result.contains("password=[REDACTED]"));
        assert!(result.contains(r#"user="john""#));
    }

    #[test]
    fn test_redact_text_line_unquoted_value() {
        let input = "2026-01-01T00:00:00Z  INFO target: msg token=abc123 count=42";
        let result = mask_text(input, &matcher(&["token"]), REDACTED);
        assert!(!result.contains("abc123"));
        assert!(result.contains("token=[REDACTED]"));
        assert!(result.contains("count=42"));
    }

    #[test]
    fn test_redact_text_line_no_sensitive_fields() {
        let input = "2026-01-01T00:00:00Z  INFO target: hello username=john count=42";
        let result = mask_text(input, &matcher(&["password"]), REDACTED);
        assert_eq!(result, input);
    }

    #[test]
    fn test_redact_text_line_case_insensitive() {
        let input = r#"2026-01-01T00:00:00Z  INFO target: msg PASSWORD="secret""#;
        let result = mask_text(input, &matcher(&["password"]), REDACTED);
        assert!(!result.contains("secret"));
        assert!(result.contains("[REDACTED]"));
    }

    #[test]
    fn test_redact_text_line_multiple_sensitive() {
        let input = r#"password="pass1" host=localhost token=tok123"#;
        let result = mask_text(input, &matcher(&["password", "token"]), REDACTED);
        assert!(!result.contains("pass1"));
        assert!(!result.contains("tok123"));
        assert!(result.contains("host=localhost"));
        assert_eq!(result.matches("[REDACTED]").count(), 2);
    }

    #[test]
    fn a_log_target_path_is_not_a_key() {
        let input = "2026-01-01T00:00:00Z  INFO scalo::auth::source: refreshed in 3ms token=abc";
        let result = mask_text(input, CredentialMatcher::default_terms(), REDACTED);
        assert_eq!(
            result,
            "2026-01-01T00:00:00Z  INFO scalo::auth::source: refreshed in 3ms token=[REDACTED]"
        );
        let input = "WARN app::secrets: loaded 3 keys";
        assert_eq!(
            mask_text(input, CredentialMatcher::default_terms(), REDACTED),
            input
        );
    }

    #[test]
    fn an_escaped_quote_does_not_end_a_value() {
        let defaults = CredentialMatcher::default_terms();
        let field = r#"msg password="a\"b c" user="john""#;
        assert_eq!(
            mask_text(field, defaults, REDACTED),
            r#"msg password=[REDACTED] user="john""#
        );
        let json = r#"{"password": "a\"b"}"#;
        assert_eq!(
            mask_text(json, defaults, REDACTED),
            r#"{"password": "[REDACTED]"}"#
        );
    }

    #[test]
    fn a_debug_dump_masks_wrapped_and_multi_line_values() {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Sasl {
            username: String,
            password: Option<String>,
        }
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Config {
            host: String,
            db_password: String,
            sasl: Sasl,
            api_token: Option<String>,
        }
        let secret = format!("FAKE-{}-{}", "dump", "X".repeat(12));
        let config = Config {
            host: "db.internal".into(),
            db_password: secret.clone(),
            sasl: Sasl {
                username: "svc".into(),
                password: Some(format!("{secret} with spaces")),
            },
            api_token: Some(secret.clone()),
        };
        let fields = default_sensitive_fields();
        let patterns: Vec<&str> = fields.iter().map(String::as_str).collect();
        for dump in [format!("{config:?}"), format!("{config:#?}")] {
            let masked = mask_sensitive_string(&dump, &patterns);
            assert!(!masked.contains("FAKE-dump"), "{masked}");
            assert!(masked.contains("db.internal"), "{masked}");
            assert!(masked.contains("\"svc\""), "{masked}");
        }
    }

    // --- MaskingWriter tests ---

    #[test]
    fn test_masking_writer_passthrough_when_empty() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        {
            let mut writer = writer(&buf, &[], false);
            io::Write::write_all(&mut writer, b"password=secret\n").unwrap();
        }
        let guard = buf.lock().unwrap();
        let output = String::from_utf8_lossy(&guard);
        assert_eq!(output, "password=secret\n");
    }

    #[test]
    fn test_masking_writer_redacts_text_on_drop() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        {
            let mut writer = writer(&buf, &["password"], false);
            io::Write::write_all(&mut writer, b"password=secret123 user=john\n").unwrap();
        }
        let guard = buf.lock().unwrap();
        let output = String::from_utf8_lossy(&guard);
        assert!(output.contains("[REDACTED]"));
        assert!(!output.contains("secret123"));
        assert!(output.contains("user=john"));
    }

    #[test]
    fn test_masking_writer_redacts_json_on_drop() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        {
            let mut writer = writer(&buf, &["password"], true);
            let json = b"{\"message\":\"hello\",\"password\":\"secret123\"}\n";
            io::Write::write_all(&mut writer, json).unwrap();
        }
        let guard = buf.lock().unwrap();
        let output = String::from_utf8_lossy(&guard);
        assert!(output.contains("[REDACTED]"));
        assert!(!output.contains("secret123"));
        assert!(output.contains("hello"));
    }

    #[test]
    fn a_coloured_text_line_is_masked_and_a_clean_one_keeps_its_colour() {
        let coloured =
            "\x1b[33m WARN\x1b[0m login \x1b[3mpassword\x1b[0m\x1b[2m=\x1b[0m\"hunter2\"\n";
        let clean = "\x1b[33m WARN\x1b[0m login \x1b[3muser\x1b[0m\x1b[2m=\x1b[0m\"svc\"\n";
        for (line, expected) in [
            (coloured, " WARN login password=[REDACTED]\n"),
            (clean, clean),
        ] {
            let buf = Arc::new(Mutex::new(Vec::new()));
            {
                let mut writer = writer(&buf, &["password"], false);
                io::Write::write_all(&mut writer, line.as_bytes()).unwrap();
            }
            assert_eq!(String::from_utf8_lossy(&buf.lock().unwrap()), expected);
        }
    }

    #[test]
    fn ansi_sequences_are_stripped_whole() {
        assert_eq!(strip_ansi("a\x1b[36;2mb\x1b[0mc"), "abc");
        assert_eq!(strip_ansi("no escapes"), "no escapes");
        assert_eq!(strip_ansi("lone \x1b escape"), "lone \x1b escape");
    }

    #[test]
    fn a_text_writer_masks_key_colon_value() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        {
            let mut writer = writer(&buf, &["password", "auth"], false);
            io::Write::write_all(&mut writer, b"INFO app: retry password: hunter2 later\n")
                .unwrap();
        }
        let guard = buf.lock().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&guard),
            "INFO app: retry password: [REDACTED] later\n"
        );
    }
}
