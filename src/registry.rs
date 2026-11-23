// Project:   scalo
// File:      src/registry.rs
// Purpose:   Process-wide registry of constructors keyed by a config type name
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Constructors an app registers under a type name, so config naming that type
//! builds it the way scalo builds its own.
//!
//! [`Registry<C>`] is generic over what it holds. Each kind of pluggable piece
//! keeps one `static` registry of its constructor type, and its factory looks a
//! config's `type` up there when the name is not a built-in one. The transport
//! senders use it through [`register_sender`](crate::transport::register_sender).
//!
//! ```
//! use std::sync::Arc;
//! use scalo::registry::Registry;
//!
//! type Greeter = dyn Fn(&str) -> String + Send + Sync;
//! static GREETERS: Registry<Greeter> = Registry::new("greeter", &["plain"]);
//!
//! GREETERS.register("shouting", Arc::new(|name: &str| format!("HELLO {name}"))).unwrap();
//! let shout = GREETERS.get("shouting").unwrap();
//! assert_eq!(shout("ada"), "HELLO ada");
//! assert!(GREETERS.register("plain", Arc::new(|n: &str| n.to_string())).is_err());
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};

/// Why a name could not be registered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    /// The name is not lowercase ASCII letters, digits and `_`, starting with a letter.
    #[error(
        "{kind} type name '{name}' is invalid: use lowercase letters, digits and '_', starting with a letter, so the name also works as an env var segment"
    )]
    InvalidName {
        /// What the registry holds.
        kind: &'static str,
        /// The name refused.
        name: String,
    },
    /// The name belongs to a built-in type.
    #[error("{kind} type name '{name}' is built in and cannot be registered")]
    Reserved {
        /// What the registry holds.
        kind: &'static str,
        /// The name refused.
        name: String,
    },
    /// Something is already registered under the name.
    #[error("{kind} type name '{name}' is already registered")]
    Duplicate {
        /// What the registry holds.
        kind: &'static str,
        /// The name refused.
        name: String,
    },
}

/// Constructors of type `C`, keyed by the config type name that selects them.
///
/// Built in a `static` with [`new`](Self::new). Registration takes a write lock
/// and lookups a read lock, so register at startup before building from config.
pub struct Registry<C: ?Sized> {
    kind: &'static str,
    reserved: &'static [&'static str],
    entries: RwLock<BTreeMap<String, Arc<C>>>,
}

impl<C: ?Sized> Registry<C> {
    /// An empty registry of `kind` (named in errors and logs) that refuses the
    /// built-in names in `reserved`.
    #[must_use]
    pub const fn new(kind: &'static str, reserved: &'static [&'static str]) -> Self {
        Self {
            kind,
            reserved,
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    /// What this registry holds, as named in its errors.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// Register `constructor` under `name`.
    ///
    /// # Errors
    ///
    /// [`RegistryError`] when `name` is malformed, built in, or taken.
    pub fn register(&self, name: &str, constructor: Arc<C>) -> Result<(), RegistryError> {
        let refused = |make: fn(&'static str, String) -> RegistryError| {
            let error = make(self.kind, name.to_string());
            #[cfg(feature = "tracing")]
            tracing::error!(kind = self.kind, name, error = %error, "registration refused");
            Err(error)
        };
        if !valid_name(name) {
            return refused(|kind, name| RegistryError::InvalidName { kind, name });
        }
        if self.reserved.contains(&name) {
            return refused(|kind, name| RegistryError::Reserved { kind, name });
        }
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        if entries.contains_key(name) {
            drop(entries);
            return refused(|kind, name| RegistryError::Duplicate { kind, name });
        }
        entries.insert(name.to_string(), constructor);
        #[cfg(feature = "tracing")]
        tracing::info!(kind = self.kind, name, "type registered");
        Ok(())
    }

    /// The constructor registered under `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<C>> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    /// Every registered name, sorted.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }
}

impl<C: ?Sized> std::fmt::Debug for Registry<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("kind", &self.kind)
            .field("reserved", &self.reserved)
            .field("names", &self.names())
            .finish_non_exhaustive()
    }
}

/// Lowercase ASCII letters, digits and `_`, starting with a letter.
fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    type Make = dyn Fn() -> u32 + Send + Sync;

    #[test]
    fn registration_accepts_a_good_name_once_and_refuses_the_rest() {
        let registry: Registry<Make> = Registry::new("widget", &["builtin"]);
        let one: Arc<Make> = Arc::new(|| 1);
        // (case, name, accepted)
        let cases = [
            ("plain", "audit_log", true),
            ("digits", "sink2", true),
            ("duplicate", "audit_log", false),
            ("reserved", "builtin", false),
            ("empty", "", false),
            ("leading digit", "2sink", false),
            ("hyphen", "audit-log", false),
            ("upper case", "Audit", false),
            ("dot", "a.b", false),
        ];
        for (case, name, accepted) in cases {
            assert_eq!(
                registry.register(name, Arc::clone(&one)).is_ok(),
                accepted,
                "{case}: {name}"
            );
        }
        assert_eq!(registry.names(), ["audit_log", "sink2"]);
        assert_eq!(registry.get("sink2").map(|make| make()), Some(1));
        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn each_refusal_names_the_kind_and_the_name() {
        let registry: Registry<Make> = Registry::new("widget", &["builtin"]);
        registry.register("taken", Arc::new(|| 0)).unwrap();
        for name in ["Bad", "builtin", "taken"] {
            let message = registry
                .register(name, Arc::new(|| 0))
                .unwrap_err()
                .to_string();
            assert!(
                message.contains("widget") && message.contains(name),
                "{message}"
            );
        }
    }
}
