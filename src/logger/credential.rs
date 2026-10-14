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

/// The longest `Name(...)` value read as one value, in bytes; one that does not close within it is masked to the end of the text, and the bound keeps a line of unclosed parentheses linear.
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
    let mut out: Option<String> = None;
    let mut copied = 0;
    for (head, colon, at) in url_userinfo(text) {
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
    }
    finish(text, out, copied)
}

/// Every non-empty URL password in `text`, in order, as [`mask_url_userinfo`] masks them.
///
/// Both read the text with one scanner, so a password listed here is exactly
/// one the masker replaces. Code that checks or refuses a URL password needs
/// no pattern of its own.
///
/// # Examples
///
/// ```
/// use scalo::logger::url_passwords;
///
/// let line = "a://u:p@ss/w@db/app b://v:@h c://git@h";
/// assert_eq!(url_passwords(line), ["p@ss/w"]);
/// ```
#[must_use]
pub fn url_passwords(text: &str) -> Vec<&str> {
    url_userinfo(text)
        .filter(|&(_, colon, at)| at > colon + 1)
        .map(|(_, colon, at)| &text[colon + 1..at])
        .collect()
}

/// Where the `://`, the `:` ending the user, and the `@` ending the password sit, for each URL in `text`.
///
/// The password runs from the user's `:` to the last `@` a host follows,
/// never past white space or the next `://`. The next URL is looked for from
/// that `@`.
fn url_userinfo(text: &str) -> impl Iterator<Item = (usize, usize, usize)> + '_ {
    let bytes = text.as_bytes();
    let mut from = 0;
    std::iter::from_fn(move || {
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
            from = at;
            return Some((head, colon, at));
        }
        None
    })
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

/// The end of the quoted string opening at `from` at its next quote, escaped or not.
///
/// `None` when no quote follows, or when `multiline` is false and a line ends first.
fn plain_quoted_end(text: &str, from: usize, multiline: bool) -> Option<usize> {
    let quote = char::from(text.as_bytes()[from]);
    let rest = &text[from + 1..];
    let close = if multiline {
        rest.find(quote)
    } else {
        rest.find([quote, '\n'])
    }?;
    (rest.as_bytes()[close] != b'\n').then_some(from + 1 + close + 1)
}

/// The end of the quoted value opening at `from`: its first quote no `\` escapes, else its next quote.
///
/// The fallback keeps a value whose escape swallows its closing quote, as in
/// `"abc\"`, from masking less than a reading that ignores escapes.
fn quoted_value_end(text: &str, from: usize, multiline: bool) -> Option<usize> {
    quoted_end(text, from, multiline).or_else(|| plain_quoted_end(text, from, multiline))
}

/// The last `quote` in `text` that no backslash escapes, so a string can close at it.
fn last_closable(text: &str, quote: u8) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut end = bytes.len();
    while let Some(at) = bytes[..end].iter().rposition(|&b| b == quote) {
        let escapes = bytes[..at]
            .iter()
            .rev()
            .take_while(|&&b| b == b'\\')
            .count();
        if escapes.is_multiple_of(2) {
            return Some(at);
        }
        end = at;
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

/// The end of the unquoted value after a colon at `from`, which must end at white space, `,`, `}` or the end of the text.
fn unquoted_colon_end(text: &str, from: usize) -> Option<usize> {
    let run_end = scan_until(text, from, ends_colon_value);
    let clean_end = |at: usize| {
        text[at..]
            .chars()
            .next()
            .is_none_or(|c| is_space(c) || matches!(c, ',' | '}'))
    };
    (run_end > from)
        .then(|| {
            digit_group_ends(text, run_end, ends_colon_value)
                .into_iter()
                .rev()
                .find(|&end| clean_end(end))
        })
        .flatten()
}

/// The end of the unquoted value after an equals sign at `from`, which may be empty.
fn unquoted_equals_end(text: &str, from: usize) -> usize {
    let run_end = scan_until(text, from, ends_equals_value);
    digit_group_ends(text, run_end, ends_equals_value)
        .last()
        .copied()
        .unwrap_or(run_end)
}

/// What replaces a span of text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cover {
    /// The mask.
    Mask,
    /// The mask inside double quotes, so a JSON line stays valid JSON.
    QuotedMask,
    /// Nothing, for a run straight after a mask that already stands for it.
    Cut,
}

impl Cover {
    /// How many bytes this cover writes in place of a span.
    fn len(self, mask: &str) -> usize {
        match self {
            Self::Mask => mask.len(),
            Self::QuotedMask => mask.len() + 2,
            Self::Cut => 0,
        }
    }
}

/// A span of text and what replaces it.
#[derive(Debug, Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
    cover: Cover,
}

/// A value as a pass reads it.
#[derive(Debug, Clone, Copy)]
struct Read {
    /// Where the pass ends the value, or `None` when it replaces nothing.
    end: Option<usize>,
    /// What replaces the value, and the run past it when the pass replaces nothing.
    cover: Cover,
    /// How far the value runs, never short of `end`.
    reach: usize,
}

impl Read {
    /// A value the pass replaces whole, up to `end`.
    fn whole(end: usize, cover: Cover) -> Self {
        Self {
            end: Some(end),
            cover,
            reach: end,
        }
    }
}

/// Reads the value after each key in one text, keys taken in rising order of position.
///
/// A quoted value that never closes runs to the end of its line, or, when its
/// reading may cross lines, as a double-quoted value after `:` and a
/// `Name(...)` value may, to the end of the text short of a line break that
/// ends it. A `Name(...)` value runs to its close, or to the end of the text
/// when it does not close within [`MAX_WRAPPER_LEN`] bytes.
struct Values<'a> {
    text: &'a str,
    /// Where the line holding the last value read on one line ends.
    line_end: Option<usize>,
    /// The last quote of each kind, `"` then `'`, that no backslash escapes, found on first need.
    closable: Option<[Option<usize>; 2]>,
}

impl<'a> Values<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            line_end: None,
            closable: None,
        }
    }

    /// Read the value after a colon at `from`.
    fn colon(&mut self, from: usize) -> Read {
        let text = self.text;
        match text.as_bytes().get(from) {
            Some(b'"') => match quoted_value_end(text, from, true) {
                Some(end) => Read::whole(end, Cover::QuotedMask),
                None => Read {
                    end: None,
                    cover: Cover::QuotedMask,
                    reach: self.text_end(),
                },
            },
            Some(b'\'') => match quoted_value_end(text, from, false) {
                Some(end) => Read::whole(end, Cover::Mask),
                None => Read {
                    end: unquoted_colon_end(text, from),
                    cover: Cover::Mask,
                    reach: self.line(from),
                },
            },
            _ => self.unquoted(from, unquoted_colon_end(text, from)),
        }
    }

    /// Read the value after an equals sign at `from`.
    fn equals(&mut self, from: usize) -> Read {
        let text = self.text;
        if matches!(text.as_bytes().get(from), Some(b'"' | b'\'')) {
            return match quoted_value_end(text, from, false) {
                Some(end) => Read::whole(end, Cover::Mask),
                None => Read {
                    end: Some(unquoted_equals_end(text, from)),
                    cover: Cover::Mask,
                    reach: self.line(from),
                },
            };
        }
        self.unquoted(from, Some(unquoted_equals_end(text, from)))
    }

    /// Read an unquoted value ending at `end`, or further when a `Name(...)` value starts at `from`.
    fn unquoted(&mut self, from: usize, end: Option<usize>) -> Read {
        let (wrapped, reach) = self.wrapper(from);
        let end = end.max(wrapped);
        let low = end.unwrap_or(from);
        Read {
            end,
            cover: Cover::Mask,
            reach: reach.map_or(low, |reach| reach.max(low)),
        }
    }

    /// Where a pass ends the `Name(...)` value at `from`, and how far the value runs.
    ///
    /// The name is ASCII letters, digits and `_`. Parentheses nest, and a
    /// string inside, single or double quoted, may span lines; it closes at its
    /// first quote no backslash escapes or, when the text holds no such quote
    /// after it, at its next quote. A pass ends the value at its close only when
    /// the name starts with a letter or `_`, every string in it closes at a
    /// quote no backslash escapes, and it closes within [`MAX_WRAPPER_LEN`]
    /// bytes; otherwise the first is `None`. The value runs to its close, or to
    /// the end of the text when it does not close within the window. Both are
    /// `None` when no `Name(` starts at `from`. The walk scans each byte of the
    /// window at most once, so a value costs at most the window.
    fn wrapper(&mut self, from: usize) -> (Option<usize>, Option<usize>) {
        let text = self.text;
        let bytes = text.as_bytes();
        let name_end = from
            + bytes[from..]
                .iter()
                .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
                .count();
        if name_end == from || bytes.get(name_end) != Some(&b'(') {
            return (None, None);
        }
        let never = (None, Some(self.text_end()));
        let mut limit = text.len().min(from + MAX_WRAPPER_LEN);
        while !text.is_char_boundary(limit) {
            limit -= 1;
        }
        let window = &text[..limit];
        let mut escaped = false;
        let mut depth = 0usize;
        let mut at = name_end;
        while at < limit {
            match bytes[at] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        let end = at + 1;
                        let replaced = !(escaped || bytes[from].is_ascii_digit());
                        return (replaced.then_some(end), Some(end));
                    }
                }
                quote @ (b'"' | b'\'') => {
                    let close = if self.closes_after(at) {
                        quoted_end(window, at, true)
                    } else {
                        escaped = true;
                        window[at + 1..]
                            .find(char::from(quote))
                            .map(|i| at + 1 + i + 1)
                    };
                    let Some(close) = close else {
                        return never;
                    };
                    at = close - 1;
                }
                _ => {}
            }
            at += 1;
        }
        never
    }

    /// Whether a quote like the one at `at` that no backslash escapes lies anywhere after it.
    fn closes_after(&mut self, at: usize) -> bool {
        let text = self.text;
        let closable = *self
            .closable
            .get_or_insert_with(|| [last_closable(text, b'"'), last_closable(text, b'\'')]);
        let kind = usize::from(text.as_bytes()[at] == b'\'');
        closable[kind].is_some_and(|last| last > at)
    }

    /// Where a value whose reading may cross lines runs to: the end of the text, short of a line break that ends it.
    fn text_end(&self) -> usize {
        self.text
            .strip_suffix('\n')
            .map_or(self.text.len(), str::len)
    }

    /// Where the line holding `from` ends: its line break, or the end of the text.
    fn line(&mut self, from: usize) -> usize {
        if let Some(end) = self.line_end
            && from <= end
        {
            return end;
        }
        let end = self.text[from..]
            .find('\n')
            .map_or(self.text.len(), |i| from + i);
        self.line_end = Some(end);
        end
    }
}

/// The value after every `separator` key that `matcher` names a credential, and the run past each one.
///
/// Only a credential key's value is consumed, so a key inside another pair's
/// value, as in `config:password:x`, is still judged.
fn pair_spans(
    text: &str,
    separator: Separator,
    matcher: &CredentialMatcher,
) -> (Vec<Span>, Vec<Span>) {
    let mut values = Values::new(text);
    let mut spans = Vec::new();
    let mut runs = Vec::new();
    let mut copied = 0;
    let mut from = 0;
    while let Some(head) = next_head(text, from, separator) {
        from = head.end;
        if head.start < copied || !matcher.matches(&text[head.start..head.key_end], "") {
            continue;
        }
        let read = match separator {
            Separator::Colon => values.colon(head.end),
            Separator::Equals => values.equals(head.end),
        };
        let low = read.end.unwrap_or(head.end);
        if let Some(end) = read.end {
            spans.push(Span {
                start: head.end,
                end,
                cover: read.cover,
            });
            copied = end;
        }
        if read.reach > low {
            let cover = if read.end.is_some() {
                Cover::Cut
            } else {
                read.cover
            };
            runs.push(Span {
                start: low,
                end: read.reach,
                cover,
            });
        }
    }
    (spans, runs)
}

/// `runs`, placed in the text `spans` were read from, as they sit once each span is replaced by its cover.
///
/// A run ending inside a span ends after that span's cover.
fn placed_after(runs: Vec<Span>, spans: &[Span], mask: &str) -> Vec<Span> {
    let mut cover_ends = Vec::with_capacity(spans.len());
    let mut written = 0;
    let mut copied = 0;
    for span in spans {
        written += span.start - copied + span.cover.len(mask);
        cover_ends.push(written);
        copied = span.end;
    }
    let place = |at: usize| {
        let Some(index) = spans
            .partition_point(|span| span.start <= at)
            .checked_sub(1)
        else {
            return at;
        };
        let span = spans[index];
        if at >= span.end {
            cover_ends[index] + (at - span.end)
        } else if at > span.start {
            cover_ends[index]
        } else {
            cover_ends[index] - span.cover.len(mask)
        }
    };
    runs.into_iter()
        .map(|run| Span {
            start: place(run.start),
            end: place(run.end),
            cover: run.cover,
        })
        .collect()
}

/// `text` with each span replaced by its cover, still borrowed when there are none.
///
/// Spans that overlap or touch merge, and the first of them decides the cover,
/// so a value and the run past it come out as one mask.
fn apply<'a>(text: &'a str, mut spans: Vec<Span>, mask: &str) -> Cow<'a, str> {
    if spans.is_empty() {
        return Cow::Borrowed(text);
    }
    spans.sort_by_key(|span| (span.start, span.cover == Cover::Cut));
    let mut out = String::with_capacity(text.len());
    let mut copied = 0;
    let mut spans = spans.into_iter().peekable();
    while let Some(first) = spans.next() {
        let mut end = first.end;
        while let Some(next) = spans.next_if(|next| next.start <= end) {
            end = end.max(next.end);
        }
        out.push_str(&text[copied..first.start]);
        match first.cover {
            Cover::Mask => out.push_str(mask),
            Cover::QuotedMask => {
                out.push('"');
                out.push_str(mask);
                out.push('"');
            }
            Cover::Cut => {}
        }
        copied = end;
    }
    out.push_str(&text[copied..]);
    Cow::Owned(out)
}

/// `text` with the value after every `key: value`, then every `key=value`, whose key `matcher` names a credential replaced by `mask`.
///
/// Each pass replaces a value only as far as its first reading goes, and the
/// equals pass reads the colon pass's output. The run past a value that never
/// closes, or past a `Name(...)` value that first reading does not take
/// whole, is masked after both passes, so a key inside a run is still read
/// and its value still masked.
fn mask_pairs<'a>(text: &'a str, matcher: &CredentialMatcher, mask: &str) -> Cow<'a, str> {
    let (spans, runs) = pair_spans(text, Separator::Colon, matcher);
    let runs = placed_after(runs, &spans, mask);
    let colon_masked = apply(text, spans, mask);
    let (mut spans, more) = pair_spans(&colon_masked, Separator::Equals, matcher);
    spans.extend(more);
    spans.extend(runs);
    then(colon_masked, |t| apply(t, spans, mask))
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
    then(text, |t| mask_pairs(t, matcher, mask))
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
    fn a_quoted_value_closes_at_its_first_unescaped_quote_else_its_next() {
        assert_eq!(quoted_value_end(r#""a\"b" x"#, 0, true), Some(6));
        assert_eq!(quoted_value_end(r#""abc\"}"#, 0, true), Some(6));
        assert_eq!(quoted_value_end(r"'a\' b", 0, false), Some(4));
        assert_eq!(quoted_value_end("\"a\\\nb\"", 0, true), Some(6));
        assert_eq!(quoted_value_end("\"a\\\nb\"", 0, false), None);
        assert_eq!(quoted_value_end("\"a\\\"\nb\"", 0, false), Some(4));
        assert_eq!(quoted_value_end("\"abc", 0, true), None);
        assert_eq!(quoted_value_end("\"abc\\", 0, true), None);
    }

    #[test]
    fn a_single_quoted_value_an_escape_leaves_open_masks_to_its_next_quote() {
        let defaults = CredentialMatcher::default_terms();
        assert_eq!(
            mask_text(r"password: 'a b\' next", defaults, "M"),
            "password: M next"
        );
        assert_eq!(
            mask_text(r"password='a b\' next", defaults, "M"),
            "password=M next"
        );
    }

    fn wrapper(text: &str, from: usize) -> (Option<usize>, Option<usize>) {
        Values::new(text).wrapper(from)
    }

    #[test]
    fn a_wrapper_closes_at_its_own_parenthesis() {
        assert_eq!(wrapper("Some(\"a)b\") rest", 0), (Some(11), Some(11)));
        assert_eq!(wrapper("f(g(x), y) rest", 0), (Some(10), Some(10)));
        assert_eq!(wrapper("(x)", 0), (None, None));
        assert_eq!(wrapper("name (x)", 0), (None, None));
    }

    #[test]
    fn a_wrapper_that_never_closes_runs_to_the_end_of_the_text() {
        assert_eq!(wrapper("Some(\"never closes", 0), (None, Some(18)));
        assert_eq!(wrapper("Some(a\nb\n", 0), (None, Some(8)));
    }

    #[test]
    fn a_string_with_no_unescaped_close_closes_at_its_next_quote_and_no_pass_replaces_it() {
        assert_eq!(wrapper(r#"Some("a\") x"#, 0), (None, Some(10)));
        assert_eq!(wrapper(r"T(v='a\') x", 0), (None, Some(9)));
    }

    #[test]
    fn a_name_starting_with_a_digit_runs_to_its_close_and_no_pass_replaces_it() {
        assert_eq!(wrapper("1(a b) c", 0), (None, Some(6)));
    }

    #[test]
    fn the_last_closable_quote_has_an_even_run_of_backslashes_before_it() {
        assert_eq!(last_closable(r#"a"b\"c"#, b'"'), Some(1));
        assert_eq!(last_closable(r#"a"b\\"c"#, b'"'), Some(5));
        assert_eq!(last_closable(r#"\"\""#, b'"'), None);
        assert_eq!(last_closable("abc", b'\''), None);
    }

    #[test]
    fn a_wrapped_value_closes_within_4096_bytes() {
        let defaults = CredentialMatcher::default_terms();
        for (value, whole) in [
            (format!("f({}b)", "a ".repeat(2046)), true),
            (format!("f({}bc)", "a ".repeat(2046)), false),
            (format!("f({}a)", "\u{e9} ".repeat(1364)), true),
            (format!("f({}ab)", "\u{e9} ".repeat(1364)), false),
        ] {
            let expected = if whole { "token=M next" } else { "token=M" };
            assert_eq!(
                mask_text(&format!("token={value} next"), defaults, "M"),
                expected
            );
            let reach = if whole { value.len() } else { value.len() + 5 };
            assert_eq!(
                wrapper(&format!("{value} next"), 0),
                (whole.then_some(value.len()), Some(reach))
            );
        }
    }

    /// The first quote matching the one at `at` that no backslash escapes, anywhere after it.
    fn reference_escaped_close(text: &str, at: usize) -> Option<usize> {
        let quote = char::from(text.as_bytes()[at]);
        let mut chars = text[at + 1..].char_indices();
        while let Some((i, c)) = chars.next() {
            if c == '\\' {
                chars.next();
            } else if c == quote {
                return Some(at + 1 + i);
            }
        }
        None
    }

    /// The `Name(...)` value at `from`, read one character at a time, as a check on the walk.
    fn reference_wrapper(text: &str, from: usize) -> (Option<usize>, Option<usize>) {
        let name_len = text[from..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .count();
        if name_len == 0 || !text[from + name_len..].starts_with('(') {
            return (None, None);
        }
        let never = (
            None,
            Some(text.strip_suffix('\n').map_or(text.len(), str::len)),
        );
        let mut limit = text.len().min(from + MAX_WRAPPER_LEN);
        while !text.is_char_boundary(limit) {
            limit -= 1;
        }
        let mut depth = 0;
        let mut escaped = false;
        let mut at = from + name_len;
        while at < limit {
            let c = text[at..].chars().next().unwrap_or_default();
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let replaced = !escaped && !text.as_bytes()[from].is_ascii_digit();
                        return (replaced.then_some(at + 1), Some(at + 1));
                    }
                }
                '"' | '\'' => {
                    let close = reference_escaped_close(text, at).or_else(|| {
                        escaped = true;
                        text[at + 1..].find(c).map(|i| at + 1 + i)
                    });
                    match close {
                        Some(close) if close < limit => at = close,
                        _ => return never,
                    }
                }
                _ => {}
            }
            at += c.len_utf8();
        }
        never
    }

    const WRAPPER_ALPHABET: [&str; 17] = [
        "a", "_", "1", "(", "(", ")", ")", "\"", "\"", "'", "'", "\\", "\\", " ", "\n", "x(",
        "\u{e9}",
    ];

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(5_000))]

        #[test]
        fn a_wrapped_value_ends_where_a_character_scan_ends_it(
            parts in proptest::collection::vec(proptest::sample::select(WRAPPER_ALPHABET.to_vec()), 0..40)
        ) {
            let text = parts.concat();
            for (from, _) in text.char_indices() {
                proptest::prop_assert_eq!(
                    Values::new(&text).wrapper(from),
                    reference_wrapper(&text, from),
                    "{:?} at {}", text, from
                );
            }
        }
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
