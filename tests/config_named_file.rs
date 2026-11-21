// Project:   scalo
// File:      tests/config_named_file.rs
// Purpose:   A named config file is the only config file read
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A service that names its config file reads nothing else from disk: not the
//! working directory, not `./config/`, not the user's config directory.
//!
//! The test moves the working directory and sets the global cascade, so it is
//! the only test in this binary.

#![cfg(feature = "config")]

use std::path::Path;

use scalo::config::flat_env::{self, ApplyFlatEnv, Normalize};
use scalo::config::{self, Config, ConfigOptions};
use serde::{Deserialize, Serialize};

const PREFIX: &str = "NAMEDFILE";
const APP: &str = "named-file-app";

/// One key from each place the standard search reads, and one from the file.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct Settings {
    named: Option<String>,
    cwd: Option<String>,
    config_dir: Option<String>,
    defaults: Option<String>,
    user_dir: Option<String>,
}

impl ApplyFlatEnv for Settings {
    fn apply_flat_env(&mut self, _prefix: &str) {}
}

impl Normalize for Settings {}

fn write(dir: &Path, name: &str, text: &str) {
    std::fs::create_dir_all(dir).expect("create the fixture directory");
    std::fs::write(dir.join(name), text).expect("write the fixture");
}

/// The keys the standard search finds, where the named file has none.
fn searched_keys(settings: &Settings) -> [Option<&str>; 3] {
    [
        settings.cwd.as_deref(),
        settings.config_dir.as_deref(),
        settings.defaults.as_deref(),
    ]
}

#[test]
fn a_named_file_is_the_only_config_file_read() {
    let root = tempfile::tempdir().expect("tempdir");
    let cwd = root.path().join("cwd");
    let user = root.path().join("user");
    write(&cwd, "settings.yaml", "cwd: here\n");
    write(&cwd, "defaults.yaml", "defaults: here\n");
    write(&cwd.join("config"), "settings.yaml", "config_dir: here\n");
    write(&user.join(APP), "settings.yaml", "user_dir: here\n");
    write(root.path(), "named.yaml", "named: here\n");
    let named = root.path().join("named.yaml");
    std::env::set_current_dir(&cwd).expect("enter the fixture directory");

    let vars = [
        ("XDG_CONFIG_HOME", Some(user.to_str().expect("UTF-8 path"))),
        ("APP_NAME", Some(APP)),
    ];
    temp_env::with_vars(vars, || {
        let options = |search_standard_paths: bool| ConfigOptions {
            env_prefix: PREFIX.into(),
            config_paths: vec![named.clone()],
            search_standard_paths,
            load_dotenv: false,
            ..Default::default()
        };

        // With the search on, the cascade finds every file, which shows the
        // fixture is where the search looks.
        let searched = Config::new(options(true)).expect("cascade with the search on");
        for key in ["named", "cwd", "config_dir", "defaults"] {
            assert_eq!(searched.get_string(key).as_deref(), Some("here"), "{key}");
        }
        #[cfg(target_os = "linux")]
        assert_eq!(searched.get_string("user_dir").as_deref(), Some("here"));

        let unsearched = Config::new(options(false)).expect("cascade with the search off");
        assert_eq!(unsearched.get_string("named").as_deref(), Some("here"));
        for key in ["cwd", "config_dir", "defaults", "user_dir"] {
            assert_eq!(unsearched.get_string(key), None, "{key} was read");
        }

        // `load_config` searches without a file and does not with one.
        let found: Settings = flat_env::load_config(None, PREFIX).expect("load_config, no file");
        assert_eq!(searched_keys(&found), [Some("here"); 3]);
        let named_only: Settings =
            flat_env::load_config(named.to_str(), PREFIX).expect("load_config, named file");
        assert_eq!(named_only.named.as_deref(), Some("here"));
        assert_eq!(searched_keys(&named_only), [None; 3]);
        assert_eq!(named_only.user_dir, None);

        // `load_typed` sets the global cascade, so it runs last.
        let typed: Settings =
            config::load_typed(named.to_str(), PREFIX).expect("load_typed, named file");
        assert_eq!(typed.named.as_deref(), Some("here"));
        let global = config::get();
        assert_eq!(global.get_string("named").as_deref(), Some("here"));
        for key in ["cwd", "config_dir", "defaults", "user_dir"] {
            assert_eq!(
                global.get_string(key),
                None,
                "{key} reached the global cascade"
            );
        }
    });
}
