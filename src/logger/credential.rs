// Project:   scalo
// File:      src/logger/credential.rs
// Purpose:   Decide which field names hold credentials and mask them in text
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Credential-name matching and text masking.
//!
//! A field name holds a credential when one of its terms sits anywhere in it,
//! case-insensitively, read both as written and as its words joined by `_`. So
//! `db_password`, `PGPASSWORD`, `x-api-key` and `secretAccessKey` all match, and
//! so do `tokenizer` and `mapping`: the substring rule over-masks by design.
//!
//! [`mask_text`] applies that rule to free text: every URL password, every bearer
//! token, and the value after every `key: value` or `key=value` whose key names a
//! credential.

use std::borrow::Cow;
use std::sync::LazyLock;

/// Terms that make a field name a credential wherever one sits in it.
pub(super) const SENSITIVE_FIELDS: &[&str] = &[
    // Passwords
    "password",
    "passwd",
    "pwd",
    "pass",
    "passphrase",
    // Tokens
    "token",
    "access_token",
    "refresh_token",
    "id_token",
    "session_token",
    "api_token",
    "bearer",
    // API keys
    "api_key",
    "apikey",
    "api-key",
    "api_secret",
    // Secrets
    "secret",
    "client_secret",
    "secret_key",
    "private_key",
    "privatekey",
    "secret_access_key",
    "aws_secret_access_key",
    "aws_secret",
    "access_key",
    "access_key_id",
    // Keys a qualifying word makes a credential, where `routing_key` is not one
    "account_key",
    "azure_key",
    "client_key",
    "encryption_key",
    "gcp_key",
    "hmac_key",
    "integration_key",
    "jwt_key",
    "licence_key",
    "license_key",
    "master_key",
    "sasl_key",
    "shared_key",
    "signing_key",
    "ssh_key",
    "ssl_key",
    "tls_key",
    // Auth
    "authorization",
    "auth",
    "jaas_config",
    "header_value",
    "headers_value",
    // Session
    "session",
    "session_id",
    "sessionid",
    "cookie",
    "csrf",
    // Certificates
    "certificate",
    "cert",
    "ssl_cert",
    "private",
    // Credentials
    "credential",
    "credentials",
    "creds",
    // Database
    "db_password",
    "database_password",
    "connection_string",
    // Personal data
    "ssn",
    "tfn",
    "medicare",
    "credit_card",
    "creditcard",
    "card_number",
    "cvv",
    "pin",
    "email",
    "phone",
];

/// Names that say nothing until their parent qualifies them, as `tls` does `key`.
const BARE_NAMES: [&str; 4] = ["key", "keys", "value", "values"];

/// The longest `Name(...)` value read as one value; a longer one is read as unquoted text, which keeps a line of unclosed parentheses linear.
const MAX_WRAPPER_LEN: usize = 4096;

/// The matcher over [`SENSITIVE_FIELDS`].
static DEFAULT_MATCHER: LazyLock<CredentialMatcher> =
    LazyLock::new(|| CredentialMatcher::new(SENSITIVE_FIELDS));

/// Decides whether a field name holds a credential.
#[derive(Debug, Clone)]
pub(super) struct CredentialMatcher {
    /// Lower-cased spellings of every term, none holding another.
    spellings: Vec<String>,
}

impl CredentialMatcher {
    /// Build a matcher over `terms`, each read as written and word by word.
    pub(super) fn new<I, S>(terms: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut all: Vec<String> = terms
            .into_iter()
            .flat_map(|term| {
                let (lower, joined) = spellings(term.as_ref());
                [lower, joined]
            })
            .filter(|spelling| !spelling.is_empty())
            .collect();
        all.sort_unstable();
        all.dedup();
        // A spelling that holds a shorter one can never be the only spelling that matches.
        let spellings = all
            .iter()
            .filter(|long| {
                !all.iter()
                    .any(|short| short.len() < long.len() && long.contains(short.as_str()))
            })
            .cloned()
            .collect();
        Self { spellings }
    }

    /// The matcher over the default terms.
    pub(super) fn default_terms() -> &'static Self {
        &DEFAULT_MATCHER
    }

    /// `true` when the matcher has no terms, so nothing is a credential.
    pub(super) fn is_empty(&self) -> bool {
        self.spellings.is_empty()
    }

    /// Whether `name`, held under `parent`, carries a term.
    ///
    /// `parent` counts only for a bare `key`, `keys`, `value` or `values`.
    pub(super) fn matches(&self, name: &str, parent: &str) -> bool {
        let (mut lower, mut joined) = spellings(name);
        if !parent.is_empty() && BARE_NAMES.contains(&joined.as_str()) {
            (lower, joined) = spellings(&format!("{parent}_{name}"));
        }
        self.spellings
            .iter()
            .any(|spelling| lower.contains(spelling.as_str()) || joined.contains(spelling.as_str()))
    }
}

/// `name` lower-cased as written, and as its words joined by `_`.
///
/// The second form reads `secretAccessKey`, `x-api-key` and `ssl.key.pem` as
/// `secret_access_key`, `x_api_key` and `ssl_key_pem`.
fn spellings(name: &str) -> (String, String) {
    (name.to_lowercase(), joined_words(name))
}

/// The ASCII words of `name`, lower-cased and joined by `_`.
///
/// A word is an acronym, a capitalised or lower-case run, or a run of digits, so
/// `HTTPServer` is `http_server`; anything else separates words.
fn joined_words(name: &str) -> String {
    let bytes = name.as_bytes();
    let mut out = String::with_capacity(name.len());
    let mut start = 0;
    while start < bytes.len() {
        let end = word_end(bytes, start);
        if end == start {
            start += 1;
            continue;
        }
        if !out.is_empty() {
            out.push('_');
        }
        out.extend(
            bytes[start..end]
                .iter()
                .map(|b| char::from(b.to_ascii_lowercase())),
        );
        start = end;
    }
    out
}

/// Where the word starting at `start` ends, or `start` when no word starts there.
fn word_end(bytes: &[u8], start: usize) -> usize {
    let run = |from: usize, class: fn(&u8) -> bool| {
        from + bytes[from..].iter().take_while(|&b| class(b)).count()
    };
    let first = bytes[start];
    if first.is_ascii_uppercase() {
        let upper_end = run(start, u8::is_ascii_uppercase);
        if bytes.get(upper_end).is_some_and(u8::is_ascii_lowercase) {
            // An acronym gives its last capital to the capitalised word after it.
            if upper_end - start >= 2 {
                return upper_end - 1;
            }
            return run(upper_end, u8::is_ascii_lowercase);
        }
        return upper_end;
    }
    if first.is_ascii_lowercase() {
        return run(start, u8::is_ascii_lowercase);
    }
    if first.is_ascii_digit() {
        return run(start, u8::is_ascii_digit);
    }
    start
}

/// Whether a field called `name` holds a credential.
///
/// A name is a credential when one of the [`default_sensitive_fields`] sits
/// anywhere in it, case-insensitively. Separators and case changes do not hide a
/// term, so `db_password`, `PGPASSWORD`, `x-api-key` and `secretAccessKey` all
/// match. A name that only contains a term matches too, so `tokenizer` and
/// `mapping` are credentials.
///
/// `parent` is the name of the object or array holding the field. It counts only
/// for a bare `key`, `keys`, `value` or `values`, so a `key` under `tls` is a
/// credential and one under `kafka` is not. Pass `""` for a field with no parent.
///
/// [`default_sensitive_fields`]: crate::logger::default_sensitive_fields
///
/// # Examples
///
/// ```
/// use scalo::logger::is_credential_name;
///
/// assert!(is_credential_name("secretAccessKey", ""));
/// assert!(is_credential_name("key", "tls"));
/// assert!(!is_credential_name("key", "kafka"));
/// assert!(!is_credential_name("routing_key", ""));
/// ```
#[must_use]
pub fn is_credential_name(name: &str, parent: &str) -> bool {
    CredentialMatcher::default_terms().matches(name, parent)
}

/// White space as the masking rules read it: Unicode white space and the ASCII separators U+001C to U+001F.
fn is_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// The byte offset of the first char at or after `from` for which `stop` holds, or the text's length.
fn scan_until(text: &str, from: usize, stop: impl Fn(char) -> bool) -> usize {
    text[from..]
        .char_indices()
        .find(|&(_, c)| stop(c))
        .map_or(text.len(), |(i, _)| from + i)
}

/// `text` with the password of every `scheme://user:password@host` in it replaced by `mask`.
///
/// The password runs to the last `@` a host follows, so one holding `@`, `/`,
/// `?` or `#` is masked whole. Nothing tells such a password from an `@` later
/// in the path or query, so `https://u:p@host/x?to=ops@example.com` is masked
/// through to `example.com`, and `https://host:8443/@scope/pkg` loses its port.
/// A URL with no `:` in its userinfo, such as `ssh://git@host`, is left alone.
/// The scan stops at white space and at the next `://`, so a long line costs
/// linear time.
///
/// `keep_user` keeps the user, so the text still says whose credential it was;
/// `false` replaces the whole `user:password`. `mask` is inserted as written.
///
/// # Examples
///
/// ```
/// use scalo::logger::mask_url_userinfo;
///
/// let line = "connecting to postgres://svc:p@ss/word@db:5432/app";
/// assert_eq!(
///     mask_url_userinfo(line, "***", true),
///     "connecting to postgres://svc:***@db:5432/app"
/// );
/// assert_eq!(
///     mask_url_userinfo(line, "***", false),
///     "connecting to postgres://***@db:5432/app"
/// );
/// ```
#[must_use]
pub fn mask_url_userinfo<'a>(text: &'a str, mask: &str, keep_user: bool) -> Cow<'a, str> {
    let bytes = text.as_bytes();
    let mut out: Option<String> = None;
    let mut copied = 0;
    let mut from = 0;
    while let Some(found) = text[from..].find("://") {
        let head = from + found;
        let colon = scan_until(text, head + 3, |c| {
            is_space(c) || matches!(c, ':' | '/' | '@')
        });
        if bytes.get(colon) != Some(&b':') {
            from = head + 1;
            continue;
        }
        let run_end = password_run_end(text, colon + 1);
        let Some(at) = last_host_at(text, colon + 1, run_end) else {
            from = head + 1;
            continue;
        };
        if keep_user && at > colon + 1 {
            let buf = out.get_or_insert_with(|| String::with_capacity(text.len()));
            buf.push_str(&text[copied..=colon]);
            buf.push_str(mask);
            copied = at;
        } else if !keep_user {
            let buf = out.get_or_insert_with(|| String::with_capacity(text.len()));
            buf.push_str(&text[copied..head]);
            buf.push_str("://");
            buf.push_str(mask);
            copied = at;
        }
        from = at;
    }
    finish(text, out, copied)
}

/// Where the run that may hold a password ends: the next white space, or the next `://`.
fn password_run_end(text: &str, from: usize) -> usize {
    text[from..]
        .char_indices()
        .find(|&(i, c)| is_space(c) || text[from + i..].starts_with("://"))
        .map_or(text.len(), |(i, _)| from + i)
}

/// The last `@` in `from..to` that a host follows.
fn last_host_at(text: &str, from: usize, to: usize) -> Option<usize> {
    text[from..to]
        .rmatch_indices('@')
        .map(|(i, _)| from + i)
        .find(|&at| host_follows(text, at))
}

/// Whether a host follows the `@` at `at`: no `@` in it, ending at `/`, `?`, `#`, white space or the end.
fn host_follows(text: &str, at: usize) -> bool {
    let end = scan_until(text, at + 1, |c| {
        is_space(c) || matches!(c, '@' | '/' | '?' | '#')
    });
    text[end..]
        .chars()
        .next()
        .is_none_or(|c| is_space(c) || matches!(c, '/' | '?' | '#'))
}

/// `text` with every bearer token after the word `bearer` replaced by `mask`.
fn mask_bearer<'a>(text: &'a str, mask: &str) -> Cow<'a, str> {
    let mut out: Option<String> = None;
    let mut copied = 0;
    let mut from = 0;
    while let Some(found) = find_bearer(text, from) {
        let gap_end = scan_until(text, found + 6, |c| !is_space(c));
        let token_end = scan_until(text, gap_end, is_space);
        if gap_end == found + 6 || token_end == gap_end {
            from = found + 1;
            continue;
        }
        let buf = out.get_or_insert_with(|| String::with_capacity(text.len()));
        buf.push_str(&text[copied..found]);
        buf.push_str("bearer ");
        buf.push_str(mask);
        copied = token_end;
        from = token_end;
    }
    finish(text, out, copied)
}

/// The next `bearer`, in any case, at or after `from` with no word character before it.
fn find_bearer(text: &str, from: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    (from..bytes.len()).find(|&i| {
        matches!(bytes[i], b'b' | b'B')
            && text
                .get(i..i + 6)
                .is_some_and(|w| w.eq_ignore_ascii_case("bearer"))
            && text[..i]
                .chars()
                .next_back()
                .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
    })
}

/// The separator between a key and its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Separator {
    /// `key: value` (JSON, YAML, headers, Python and Rust `Debug`).
    Colon,
    /// `key=value` (env, form data, TOML, INI, `tracing` text fields).
    Equals,
}

/// One `key<separator>` found in text.
#[derive(Debug, Clone, Copy)]
struct Head {
    /// Where the key starts.
    start: usize,
    /// Where the key ends.
    key_end: usize,
    /// Where the value starts, past the separator and the space around it.
    end: usize,
}

/// A character that can be part of a key in text.
fn is_key_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-')
}

/// The next key followed by `separator` that starts at or after `from`.
fn next_head(text: &str, from: usize, separator: Separator) -> Option<Head> {
    let bytes = text.as_bytes();
    let mut start = from;
    while start < bytes.len() {
        if !is_key_byte(bytes[start]) || (start > 0 && is_key_byte(bytes[start - 1])) {
            start += 1;
            continue;
        }
        let key_end = start
            + bytes[start..]
                .iter()
                .take_while(|&&b| is_key_byte(b))
                .count();
        if let Some(end) = separator_end(text, start, key_end, separator) {
            return Some(Head {
                start,
                key_end,
                end,
            });
        }
        start = key_end;
    }
    None
}

/// Where the value starts when `separator` follows the key at `start..key_end`.
fn separator_end(text: &str, start: usize, key_end: usize, separator: Separator) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut at = key_end;
    if matches!(bytes.get(at), Some(b'"' | b'\'')) {
        at += 1;
    }
    match separator {
        Separator::Colon => {
            at = scan_until(text, at, |c| !is_space(c));
            if bytes.get(at) != Some(&b':') {
                return None;
            }
            // `a::b` is a Rust path, such as a log target, never a key and a value.
            if bytes.get(at + 1) == Some(&b':') || (start >= 2 && &bytes[start - 2..start] == b"::")
            {
                return None;
            }
            Some(scan_until(text, at + 1, |c| !is_space(c)))
        }
        Separator::Equals => {
            at = scan_until(text, at, |c| !matches!(c, ' ' | '\t'));
            if bytes.get(at) != Some(&b'=') || matches!(bytes.get(at + 1), Some(b'=' | b'>')) {
                return None;
            }
            Some(scan_until(text, at + 1, |c| !matches!(c, ' ' | '\t')))
        }
    }
}

/// The end of the quoted string opening at `from`, honouring `\` escapes.
///
/// `None` when it never closes, or when `multiline` is false and a line ends first.
fn quoted_end(text: &str, from: usize, multiline: bool) -> Option<usize> {
    let quote = char::from(text.as_bytes()[from]);
    let mut chars = text[from + 1..].char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some((_, '\n')) if !multiline => return None,
                Some(_) => {}
                None => return None,
            },
            '\n' if !multiline => return None,
            c if c == quote => return Some(from + 1 + i + 1),
            _ => {}
        }
    }
    None
}

/// The end of a `Name(...)` value such as `Some("...")`, with nested parentheses and quotes honoured.
fn wrapper_end(text: &str, from: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let first = *bytes.get(from)?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let name_end = from
        + bytes[from..]
            .iter()
            .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
            .count();
    if bytes.get(name_end) != Some(&b'(') {
        return None;
    }
    let mut limit = text.len().min(from + MAX_WRAPPER_LEN);
    while !text.is_char_boundary(limit) {
        limit -= 1;
    }
    let window = &text[..limit];
    let mut depth = 0usize;
    let mut at = name_end;
    while at < limit {
        match bytes[at] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(at + 1);
                }
            }
            b'"' | b'\'' => at = quoted_end(window, at, true)? - 1,
            _ => {}
        }
        at += 1;
    }
    None
}

/// The end of the digit groups after `from`, as in `+61 412 345 678`, each a space, an optional `(` or `+`, a digit, then non-stop characters.
///
/// Returns every end in turn, the first being `from` itself.
fn digit_group_ends(text: &str, from: usize, stop: fn(char) -> bool) -> Vec<usize> {
    let bytes = text.as_bytes();
    let mut ends = vec![from];
    let mut at = from;
    loop {
        if bytes.get(at) != Some(&b' ') {
            break;
        }
        let mut next = at + 1;
        if matches!(bytes.get(next), Some(b'(' | b'+')) {
            next += 1;
        }
        if !bytes.get(next).is_some_and(u8::is_ascii_digit) {
            break;
        }
        at = scan_until(text, next + 1, stop);
        ends.push(at);
    }
    ends
}

/// A character that ends an unquoted value after a colon.
fn ends_colon_value(c: char) -> bool {
    is_space(c) || matches!(c, ',' | '"' | '}')
}

/// A character that ends an unquoted value after an equals sign.
fn ends_equals_value(c: char) -> bool {
    is_space(c) || c == '&'
}

/// The value after a colon at `from`, and whether the mask keeps its quotes.
fn colon_value(text: &str, from: usize) -> Option<(usize, bool)> {
    let bytes = text.as_bytes();
    if bytes.get(from) == Some(&b'"')
        && let Some(end) = quoted_end(text, from, true)
    {
        return Some((end, true));
    }
    if bytes.get(from) == Some(&b'\'')
        && let Some(end) = quoted_end(text, from, false)
    {
        return Some((end, false));
    }
    let run_end = scan_until(text, from, ends_colon_value);
    // The value must end at white space, `,`, `}` or the end of the text.
    let clean_end = |at: usize| {
        text[at..]
            .chars()
            .next()
            .is_none_or(|c| is_space(c) || matches!(c, ',' | '}'))
    };
    let unquoted = (run_end > from)
        .then(|| {
            digit_group_ends(text, run_end, ends_colon_value)
                .into_iter()
                .rev()
                .find(|&end| clean_end(end))
        })
        .flatten();
    // A `Name(...)` value such as `Some("...")` goes whole, never less than the unquoted reading.
    unquoted
        .max(wrapper_end(text, from))
        .map(|end| (end, false))
}

/// The end of the value after an equals sign at `from`, which may be empty.
fn equals_value(text: &str, from: usize) -> usize {
    let bytes = text.as_bytes();
    if matches!(bytes.get(from), Some(b'"' | b'\''))
        && let Some(end) = quoted_end(text, from, false)
    {
        return end;
    }
    let run_end = scan_until(text, from, ends_equals_value);
    let unquoted = digit_group_ends(text, run_end, ends_equals_value)
        .last()
        .copied()
        .unwrap_or(run_end);
    // A `Name(...)` value such as `Some("...")` goes whole, never less than the unquoted reading.
    wrapper_end(text, from).map_or(unquoted, |end| end.max(unquoted))
}

/// `text` with the value after every `separator` key that names a credential replaced by `mask`.
///
/// Only a credential key's value is consumed, so a key inside another pair's
/// value, as in `config:password:x`, is still judged.
fn mask_pairs<'a>(
    text: &'a str,
    separator: Separator,
    matcher: &CredentialMatcher,
    mask: &str,
) -> Cow<'a, str> {
    let mut out: Option<String> = None;
    let mut copied = 0;
    let mut from = 0;
    while let Some(head) = next_head(text, from, separator) {
        from = head.end;
        if head.start < copied || !matcher.matches(&text[head.start..head.key_end], "") {
            continue;
        }
        let value = match separator {
            Separator::Colon => colon_value(text, head.end),
            Separator::Equals => Some((equals_value(text, head.end), false)),
        };
        let Some((value_end, keep_quotes)) = value else {
            continue;
        };
        let buf = out.get_or_insert_with(|| String::with_capacity(text.len()));
        buf.push_str(&text[copied..head.end]);
        if keep_quotes {
            buf.push('"');
            buf.push_str(mask);
            buf.push('"');
        } else {
            buf.push_str(mask);
        }
        copied = value_end;
    }
    finish(text, out, copied)
}

/// `text` with every credential in it replaced by `mask`.
///
/// Masks, in order: URL passwords, bearer tokens, the value after `key: value`
/// and the value after `key=value`, where `matcher` names the key a credential.
pub(super) fn mask_text<'a>(
    text: &'a str,
    matcher: &CredentialMatcher,
    mask: &str,
) -> Cow<'a, str> {
    let text = mask_url_userinfo(text, mask, true);
    let text = then(text, |t| mask_bearer(t, mask));
    let text = then(text, |t| mask_pairs(t, Separator::Colon, matcher, mask));
    then(text, |t| mask_pairs(t, Separator::Equals, matcher, mask))
}

/// `text` after `step`, still borrowed when neither changed it.
fn then(text: Cow<'_, str>, step: impl FnOnce(&str) -> Cow<'_, str>) -> Cow<'_, str> {
    let changed = match step(&text) {
        Cow::Borrowed(_) => None,
        Cow::Owned(masked) => Some(masked),
    };
    changed.map_or(text, Cow::Owned)
}

/// The text built in `out` with the rest of `text` from `copied`, or `text` itself when nothing was built.
fn finish(text: &str, out: Option<String>, copied: usize) -> Cow<'_, str> {
    match out {
        None => Cow::Borrowed(text),
        Some(mut buf) => {
            buf.push_str(&text[copied..]);
            Cow::Owned(buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_split_on_case_digits_and_separators() {
        assert_eq!(joined_words("secretAccessKey"), "secret_access_key");
        assert_eq!(joined_words("HTTPServerKey"), "http_server_key");
        assert_eq!(joined_words("ABCdef"), "ab_cdef");
        assert_eq!(joined_words("x-api-key"), "x_api_key");
        assert_eq!(joined_words("ssl.key.pem"), "ssl_key_pem");
        assert_eq!(joined_words("s3_secret"), "s_3_secret");
        assert_eq!(joined_words("PGPASSWORD"), "pgpassword");
        assert_eq!(joined_words("__"), "");
        assert_eq!(joined_words("p\u{e4}ssword"), "p_ssword");
    }

    #[test]
    fn a_spelling_holding_a_shorter_one_is_dropped() {
        let matcher = CredentialMatcher::new(["token", "access_token", "api-key"]);
        assert_eq!(matcher.spellings, ["api-key", "api_key", "token"]);
    }

    #[test]
    fn an_empty_matcher_names_nothing() {
        let matcher = CredentialMatcher::new(Vec::<String>::new());
        assert!(matcher.is_empty());
        assert!(!matcher.matches("password", ""));
    }

    #[test]
    fn a_parent_qualifies_only_a_bare_name() {
        let matcher = CredentialMatcher::new(["tls_key"]);
        assert!(matcher.matches("KEY", "tls"));
        assert!(!matcher.matches("KEY", ""));
        assert!(!matcher.matches("keyring", "tls"));
    }

    #[test]
    fn bearer_needs_a_word_boundary_space_and_token() {
        assert_eq!(mask_bearer("Bearer abc", "M"), "bearer M");
        assert_eq!(mask_bearer("x bearer   abc def", "M"), "x bearer M def");
        assert_eq!(mask_bearer("unbearer abc", "M"), "unbearer abc");
        assert_eq!(mask_bearer("bearer", "M"), "bearer");
        assert_eq!(mask_bearer("bearer ", "M"), "bearer ");
        assert_eq!(mask_bearer("bearerabc", "M"), "bearerabc");
        assert!(matches!(
            mask_bearer("no token here", "M"),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn unchanged_text_is_not_copied() {
        let matcher = CredentialMatcher::default_terms();
        let line = "processed 1200 records from topic raw_events";
        assert!(matches!(mask_text(line, matcher, "M"), Cow::Borrowed(_)));
    }

    #[test]
    fn a_wrapper_closes_at_its_own_parenthesis() {
        assert_eq!(wrapper_end("Some(\"a)b\") rest", 0), Some(11));
        assert_eq!(wrapper_end("f(g(x), y) rest", 0), Some(10));
        assert_eq!(wrapper_end("Some(\"never closes", 0), None);
        assert_eq!(wrapper_end("(x)", 0), None);
        assert_eq!(wrapper_end("name (x)", 0), None);
    }

    #[test]
    fn digit_groups_stop_at_a_word() {
        assert_eq!(
            digit_group_ends("+61 412 345 on", 3, ends_colon_value),
            [3, 7, 11]
        );
        assert_eq!(digit_group_ends("1 (2 3", 1, ends_colon_value), [1, 4, 6]);
    }
}
