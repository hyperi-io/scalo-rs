// Project:   scalo
// File:      src/config/env_text.rs
// Purpose:   Env layers that keep each variable's text for fields that refuse its parsed value
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The cascade's env layers, with the text each variable was set to.
//!
//! figment parses an env value before any field sees it, so `0123` reaches a
//! `String` field as the integer 123, which the field refuses.
//! [`EnvText::extract`] gives a field that refuses an env var's parsed value
//! the variable's text instead.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use figment::error::{Actual, Kind};
use figment::providers::{Env, Serialized};
use figment::{Error, Figment, Provider};
use serde::de::DeserializeOwned;

/// The env layers merged into a figment, and the text each variable set.
#[derive(Default)]
pub(super) struct EnvText {
    /// Each variable's text, by the key path it sets. A later layer's text
    /// replaces an earlier one's, as its value does.
    text: HashMap<String, String>,
    /// Each env layer's metadata name, which an error carries when the value
    /// it names came from that layer.
    layers: Vec<Cow<'static, str>>,
}

impl fmt::Debug for EnvText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The text holds secrets, so only the key paths are shown.
        f.debug_struct("EnvText")
            .field("keys", &self.text.keys().collect::<Vec<&String>>())
            .field("layers", &self.layers)
            .finish()
    }
}

impl EnvText {
    /// Merge `env` onto `figment`, keeping each variable's text.
    pub(super) fn merge(&mut self, figment: Figment, env: Env) -> Figment {
        self.layers.push(env.metadata().name);
        self.text.extend(
            env.iter()
                .map(|(key, text)| (key.as_str().to_owned(), text)),
        );
        figment.merge(env)
    }

    /// Deserialise `T` from `figment`, at `key` or the root.
    ///
    /// A field that refuses the value an env var was parsed into gets the
    /// variable's text, less the surrounding whitespace figment trims from
    /// every string. Where the text is refused too, the error is the one the
    /// parsed value raised.
    pub(super) fn extract<T: DeserializeOwned>(
        &self,
        figment: &Figment,
        key: Option<&str>,
    ) -> Result<T, Error> {
        let mut figment = Cow::Borrowed(figment);
        // The path last given its text, and the error its parsed value raised.
        let mut retried: Option<(String, Error)> = None;
        loop {
            let read = match key {
                Some(key) => figment.extract_inner(key),
                None => figment.extract(),
            };
            let err = match read {
                Ok(value) => return Ok(value),
                Err(err) => err,
            };
            let path = error_path(key, &err);
            if let Some((last, parsed)) = retried.take()
                && last == path
            {
                return Err(parsed);
            }
            let Some(text) = self.text_for(&path, &err) else {
                return Err(err);
            };
            figment = Cow::Owned(figment.into_owned().merge(Serialized::default(&path, text)));
            retried = Some((path, err));
        }
    }

    /// The text to retry `path` with, when `err` is a field refusing the
    /// value an env layer parsed there.
    fn text_for(&self, path: &str, err: &Error) -> Option<&str> {
        let Kind::InvalidType(actual, _) = &err.kind else {
            return None;
        };
        let from_env = err
            .metadata
            .as_ref()
            .is_some_and(|md| self.layers.contains(&md.name));
        if !from_env || matches!(actual, Actual::Str(_)) {
            return None;
        }
        self.text
            .get(path)
            .map(|text| text.trim_matches(|c: char| c.is_ascii_whitespace()))
    }
}

/// The key path, from the root, of the value `err` names.
///
/// figment's `extract_inner` appends its key to the error's path after the
/// path inside the key.
fn error_path(key: Option<&str>, err: &Error) -> String {
    let outer: Vec<&str> =
        key.map_or_default(|key| key.split('.').filter(|part| !part.is_empty()).collect());
    let inner = err
        .path
        .len()
        .checked_sub(outer.len())
        .filter(|&at| {
            err.path[at..]
                .iter()
                .map(String::as_str)
                .eq(outer.iter().copied())
        })
        .map_or(&err.path[..], |at| &err.path[..at]);
    let parts: Vec<&str> = outer
        .iter()
        .copied()
        .chain(inner.iter().map(String::as_str))
        .collect();
    parts.join(".")
}
