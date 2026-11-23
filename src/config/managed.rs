// Project:   scalo
// File:      src/config/managed.rs
// Purpose:   Opt-in config layer from Group Policy (Windows) or MDM managed preferences (macOS)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The managed layer: settings an administrator pushes through Group Policy,
//! Intune or Jamf. It merges after every config file and before env vars, so
//! policy beats a local file edit and an admin on the box can still override
//! it. Nothing is read unless the app names a [`ManagedSource`].

use figment::value::{Dict, Map, Value};
use figment::{Error, Metadata, Profile, Provider};

use super::ConfigError;
use super::load::FileNote;

/// Where an app's managed settings live. An empty field reads nothing on its OS.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedSource {
    /// Key under `HKLM\SOFTWARE\Policies\` on Windows, such as `Example\App`.
    pub windows_key: String,
    /// Managed preferences domain on macOS, such as `com.example.app`: the
    /// machine-wide `/Library/Managed Preferences/<domain>.plist`.
    pub macos_domain: String,
}

/// A value read from a managed store, before it is mapped into the config tree.
#[cfg_attr(
    not(windows),
    allow(
        dead_code,
        reason = "the registry reader alone builds `Text`, and Linux builds none"
    )
)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Raw {
    /// Text read as the env layer reads a variable: a `REG_SZ`.
    Text(String),
    /// A scalar taken as it is.
    Scalar(Value),
    /// A `REG_MULTI_SZ` or a plist array.
    List(Vec<Raw>),
    /// A registry subkey or a plist dict.
    Section(Tree),
    /// A type with no config meaning, named for the warning.
    Unsupported(String),
}

/// Named values, as a registry key or a plist dict holds them.
type Tree = Vec<(String, Raw)>;

/// The layer as read: one figment provider, named so an error or a key's
/// metadata says which store a value came from.
#[derive(Debug, Clone)]
pub(crate) struct ManagedLayer {
    location: String,
    dict: Dict,
}

impl ManagedLayer {
    pub(super) fn new(location: String, dict: Dict) -> Self {
        Self { location, dict }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }
}

impl Provider for ManagedLayer {
    fn metadata(&self) -> Metadata {
        Metadata::named(format!("managed: {}", self.location))
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        Ok(Profile::Default.collect(self.dict.clone()))
    }
}

/// Read the layer `source` names. `None` without a source or a store.
///
/// # Errors
///
/// [`ConfigError::InvalidValue`] for a key that looks like a secret, and
/// [`ConfigError::LoadError`] for a store that exists but can't be read.
pub(crate) fn read(source: Option<&ManagedSource>) -> Result<Option<ManagedLayer>, ConfigError> {
    let Some((location, tree)) = source.map(read_store).transpose()?.flatten() else {
        return Ok(None);
    };
    let mut mapped = Mapped::default();
    let dict = map(tree, "", &mut mapped)?;
    let at = || location.clone();
    let keys = mapped.keys.len();
    FileNote::ManagedRead { source: at(), keys }.log_or_hold();
    for key in mapped.keys {
        FileNote::ManagedKey { source: at(), key }.log_or_hold();
    }
    for detail in mapped.skipped {
        FileNote::ManagedSkipped {
            source: at(),
            detail,
        }
        .log_or_hold();
    }
    Ok(Some(ManagedLayer::new(location, dict)))
}

/// What [`map`] set and skipped, for the log.
#[derive(Debug, Default)]
struct Mapped {
    keys: Vec<String>,
    skipped: Vec<String>,
}

/// Map a store's tree into the config tree. Names map as the env layer maps
/// them: lowercased, with `__` or `.` nesting.
fn map(tree: Tree, prefix: &str, out: &mut Mapped) -> Result<Dict, ConfigError> {
    let mut dict = Dict::new();
    for (name, raw) in tree {
        let path = name.to_lowercase().replace("__", ".");
        if path.split('.').any(str::is_empty) {
            out.skipped
                .push(format!("'{name}' in '{prefix}' is not a usable key name"));
            continue;
        }
        let key = if prefix.is_empty() {
            path.clone()
        } else {
            format!("{prefix}.{path}")
        };
        let leaf = !matches!(raw, Raw::Section(_));
        let Some(value) = value_of(raw, &key, out)? else {
            continue;
        };
        if leaf {
            refuse_secrets(&key, &value)?;
            out.keys.push(key);
        }
        insert(&mut dict, &path, value);
    }
    Ok(dict)
}

/// The config value of `raw`, or `None` when there is nothing to set.
fn value_of(raw: Raw, key: &str, out: &mut Mapped) -> Result<Option<Value>, ConfigError> {
    Ok(match raw {
        Raw::Text(text) => {
            let Ok(value) = text.parse::<Value>();
            Some(value)
        }
        Raw::Scalar(value) => Some(value),
        Raw::List(items) => {
            let mut list: Vec<Value> = Vec::new();
            for item in items {
                list.extend(value_of(item, key, out)?);
            }
            Some(Value::from(list))
        }
        Raw::Section(tree) => Some(map(tree, key, out)?)
            .filter(|dict| !dict.is_empty())
            .map(Value::from),
        Raw::Unsupported(kind) => {
            out.skipped
                .push(format!("{key}: {kind} is not a config type"));
            None
        }
    })
}

/// Set `value` at the dotted `path`, merging into the sections there.
fn insert(dict: &mut Dict, path: &str, value: Value) {
    let (head, value) = match path.split_once('.') {
        Some((head, rest)) => {
            let mut nested = Dict::new();
            insert(&mut nested, rest, value);
            (head, Value::from(nested))
        }
        None => (path, value),
    };
    if let Value::Dict(_, new) = &value
        && let Some(Value::Dict(_, existing)) = dict.get_mut(head)
    {
        for (name, item) in new {
            insert(existing, name, item.clone());
        }
        return;
    }
    dict.insert(head.to_string(), value);
}

/// Last key segments the layer refuses, alone or after `_`: `token`, `sasl_password`.
const SECRET_NAMES: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "credential",
    "credentials",
];

/// Refuse a secret: Group Policy's `Registry.pol` sits in SYSVOL, where every
/// domain user can read it. A name that ends `_file` or `_path` matches none
/// of [`SECRET_NAMES`], so a path to the secret may come from policy.
fn refuse_secrets(key: &str, value: &Value) -> Result<(), ConfigError> {
    match value {
        Value::Dict(_, dict) => dict
            .iter()
            .try_for_each(|(name, item)| refuse_secrets(&format!("{key}.{name}"), item)),
        Value::Array(_, items) => items.iter().try_for_each(|item| refuse_secrets(key, item)),
        _ => {
            let leaf = key.rsplit('.').next().unwrap_or(key).to_lowercase();
            let secret = SECRET_NAMES.iter().any(|name| {
                leaf.strip_suffix(name)
                    .is_some_and(|head| head.is_empty() || head.ends_with('_'))
            });
            if secret {
                return Err(ConfigError::InvalidValue {
                    key: key.to_string(),
                    reason: format!(
                        "other users can read managed policy, so it can't hold a secret: \
                         use the key's file-path form, such as {leaf}_file"
                    ),
                });
            }
            Ok(())
        }
    }
}

/// Where a store was read from, and what it holds. `None` when there is none.
type Store = Option<(String, Tree)>;

#[cfg(windows)]
fn read_store(source: &ManagedSource) -> Result<Store, ConfigError> {
    if source.windows_key.is_empty() {
        return Ok(None);
    }
    let path = format!(r"SOFTWARE\Policies\{}", source.windows_key);
    let location = format!(r"HKLM\{path}");
    match registry_tree(windows_registry::LOCAL_MACHINE, &path) {
        Ok(tree) => Ok(tree.map(|tree| (location, tree))),
        Err(e) => Err(ConfigError::LoadError {
            path: location.into(),
            message: e.to_string(),
        }),
    }
}

/// The values and subkeys under `root\path`, or `None` when the key is absent.
#[cfg(windows)]
fn registry_tree(
    root: &windows_registry::Key,
    path: &str,
) -> windows_registry::Result<Option<Tree>> {
    // HRESULT_FROM_WIN32(ERROR_FILE_NOT_FOUND)
    const NOT_FOUND: i32 = 0x8007_0002_u32.cast_signed();
    match root.open(path) {
        Ok(key) => registry_values(&key).map(Some),
        Err(e) if e.code().0 == NOT_FOUND => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(windows)]
fn registry_values(key: &windows_registry::Key) -> windows_registry::Result<Tree> {
    use windows_registry::Type;

    let mut tree = Tree::new();
    for (name, value) in key.values()? {
        let raw = match value.ty() {
            Type::U32 => Raw::Scalar(u32::try_from(value)?.into()),
            Type::U64 => Raw::Scalar(u64::try_from(value)?.into()),
            Type::String => Raw::Text(String::try_from(value)?),
            // windows-registry 0.6 splits the list's closing NULs into empty
            // items, and the format allows no empty item.
            Type::MultiString => Raw::List(
                Vec::<String>::try_from(value)?
                    .into_iter()
                    .take_while(|item| !item.is_empty())
                    .map(|item| Raw::Scalar(item.into()))
                    .collect(),
            ),
            Type::ExpandString => Raw::Unsupported("REG_EXPAND_SZ".into()),
            Type::Bytes => Raw::Unsupported("REG_BINARY".into()),
            Type::Other(ty) => Raw::Unsupported(format!("registry type {ty}")),
        };
        tree.push((name, raw));
    }
    for name in key.keys()? {
        let section = registry_values(&key.open(&name)?)?;
        tree.push((name, Raw::Section(section)));
    }
    Ok(tree)
}

#[cfg(target_os = "macos")]
fn read_store(source: &ManagedSource) -> Result<Store, ConfigError> {
    if source.macos_domain.is_empty() {
        return Ok(None);
    }
    let path = format!("/Library/Managed Preferences/{}.plist", source.macos_domain);
    Ok(plist_tree(std::path::Path::new(&path), 0)?.map(|tree| (path, tree)))
}

/// The dict in the plist at `path`. `None` when it is missing, or skipped with
/// a warning when `owner` doesn't own it or others can write it.
#[cfg(target_os = "macos")]
fn plist_tree(path: &std::path::Path, owner: u32) -> Result<Option<Tree>, ConfigError> {
    use std::os::unix::fs::MetadataExt;

    let failed = |message: String| ConfigError::LoadError {
        path: path.to_path_buf(),
        message,
    };
    // One handle for the check and the read, so the file can't be swapped between.
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(failed(e.to_string())),
    };
    let meta = file.metadata().map_err(|e| failed(e.to_string()))?;
    if meta.uid() != owner || meta.mode() & 0o022 != 0 {
        FileNote::ManagedSkipped {
            source: path.display().to_string(),
            detail: format!(
                "owned by uid {} with mode {:o}, where root must own it and no one else write it",
                meta.uid(),
                meta.mode() & 0o7777
            ),
        }
        .log_or_hold();
        return Ok(None);
    }
    let reader = std::io::BufReader::new(file);
    match plist::Value::from_reader(reader).map_err(|e| failed(e.to_string()))? {
        plist::Value::Dictionary(dict) => Ok(Some(
            dict.into_iter().map(|(k, v)| (k, from_plist(v))).collect(),
        )),
        _ => Err(failed("the top level is not a dictionary".into())),
    }
}

#[cfg(target_os = "macos")]
fn from_plist(value: plist::Value) -> Raw {
    match value {
        plist::Value::Dictionary(dict) => {
            Raw::Section(dict.into_iter().map(|(k, v)| (k, from_plist(v))).collect())
        }
        plist::Value::Array(items) => Raw::List(items.into_iter().map(from_plist).collect()),
        plist::Value::String(text) => Raw::Scalar(text.into()),
        plist::Value::Boolean(flag) => Raw::Scalar(flag.into()),
        plist::Value::Real(number) => Raw::Scalar(number.into()),
        plist::Value::Integer(number) => match (number.as_signed(), number.as_unsigned()) {
            (Some(signed), _) => Raw::Scalar(signed.into()),
            (None, Some(unsigned)) => Raw::Scalar(unsigned.into()),
            (None, None) => Raw::Unsupported("integer".into()),
        },
        plist::Value::Data(_) => Raw::Unsupported("data".into()),
        plist::Value::Date(_) => Raw::Unsupported("date".into()),
        _ => Raw::Unsupported("plist value".into()),
    }
}

#[cfg(not(any(windows, target_os = "macos")))]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the Windows and macOS readers fail, and share this signature"
)]
fn read_store(_source: &ManagedSource) -> Result<Store, ConfigError> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn text(value: &str) -> Raw {
        Raw::Text(value.into())
    }

    fn tree(entries: &[(&str, Raw)]) -> Tree {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    fn section(entries: &[(&str, Raw)]) -> Raw {
        Raw::Section(tree(entries))
    }

    /// Map `entries` from the top: the config tree and what was set and skipped.
    fn mapped(entries: &[(&str, Raw)]) -> Result<(Dict, Mapped), ConfigError> {
        let mut out = Mapped::default();
        Ok((map(tree(entries), "", &mut out)?, out))
    }

    fn as_json(dict: Dict) -> serde_json::Value {
        serde_json::to_value(dict).unwrap()
    }

    /// Each store shape maps to the config tree, and bad data is skipped.
    #[test]
    fn store_trees_map_into_the_config_tree() {
        let list =
            |items: &[&str]| Raw::List(items.iter().map(|i| Raw::Scalar((*i).into())).collect());
        let cases: Vec<(&str, Vec<(&str, Raw)>, serde_json::Value, Vec<&str>)> = vec![
            ("an empty store", vec![], json!({}), vec![]),
            (
                "REG_SZ reads as an env var does",
                vec![
                    ("Name", text("from-policy")),
                    ("Port", text("8080")),
                    ("Enabled", text("true")),
                    ("Brokers", text("[a, b]")),
                    ("Outputs", text(r#"[{url="https://a"}, {url="https://b"}]"#)),
                ],
                json!({"name": "from-policy", "port": 8080, "enabled": true, "brokers": ["a", "b"],
                       "outputs": [{"url": "https://a"}, {"url": "https://b"}]}),
                vec![],
            ),
            (
                "scalars and REG_MULTI_SZ items stay as they are",
                vec![
                    ("Workers", Raw::Scalar(4_u32.into())),
                    ("Tags", list(&["true", "8"])),
                ],
                json!({"workers": 4, "tags": ["true", "8"]}),
                vec![],
            ),
            (
                "subkeys and __ nest, merging into one section",
                vec![
                    ("Sink__Mode", text("tls")),
                    (
                        "Sink",
                        section(&[("TLS", section(&[("CA_File", text("/ca.pem"))]))]),
                    ),
                ],
                json!({"sink": {"mode": "tls", "tls": {"ca_file": "/ca.pem"}}}),
                vec![],
            ),
            (
                "unsupported types, bad names and empty sections are skipped",
                vec![
                    ("Blob", Raw::Unsupported("REG_BINARY".into())),
                    ("", text("x")),
                    ("A__", text("x")),
                    ("Empty", section(&[])),
                    (
                        "Mixed",
                        Raw::List(vec![text("a"), Raw::Unsupported("data".into())]),
                    ),
                ],
                json!({"mixed": ["a"]}),
                vec![
                    "blob: REG_BINARY is not a config type",
                    "'' in '' is not a usable key name",
                    "'A__' in '' is not a usable key name",
                    "mixed: data is not a config type",
                ],
            ),
            (
                "names that only look like secrets, and a secret's file-path form, are allowed",
                vec![
                    ("Auth", section(&[("Mode", text("basic"))])),
                    ("Max_Tokens", text("512")),
                    ("Partition_Key", text("host")),
                    ("Verify_Cert", text("true")),
                    (
                        "Tls",
                        section(&[("Key_File", text("/k.pem")), ("Password_Path", text("/p"))]),
                    ),
                ],
                json!({"auth": {"mode": "basic"}, "max_tokens": 512, "partition_key": "host",
                       "verify_cert": true, "tls": {"key_file": "/k.pem", "password_path": "/p"}}),
                vec![],
            ),
        ];
        for (name, tree, want, skipped) in cases {
            let (got, out) = mapped(&tree).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(as_json(got), want, "{name}");
            assert_eq!(out.skipped, skipped, "{name}");
        }
    }

    /// A secret anywhere in the layer fails the read, naming the key.
    #[test]
    fn a_secret_is_refused_by_name() {
        let cases: Vec<(&str, Vec<(&str, Raw)>, &str)> = vec![
            (
                "a suffix in a section",
                vec![("Kafka", section(&[("Sasl_Password", text("x"))]))],
                "kafka.sasl_password",
            ),
            ("a whole name", vec![("Api_Key", text("x"))], "api_key"),
            (
                "a suffix",
                vec![("Client_Secret", text("x"))],
                "client_secret",
            ),
            ("an __ name", vec![("Auth__Token", text("x"))], "auth.token"),
            (
                "inside REG_SZ text",
                vec![("Outputs", text("[{Private_Key=x}]"))],
                "outputs.Private_Key",
            ),
            (
                "a list",
                vec![("Credentials", Raw::List(vec![text("x")]))],
                "credentials",
            ),
        ];
        for (name, tree, key) in cases {
            match mapped(&tree) {
                Err(ConfigError::InvalidValue { key: got, reason }) => {
                    assert_eq!(got, key, "{name}");
                    assert!(reason.contains("file-path form"), "{name}: {reason}");
                }
                other => panic!("{name}: expected a refusal, got {other:?}"),
            }
        }
    }

    /// The layer beats every file and loses to env vars, and figment names it
    /// as the source of the keys it set.
    #[test]
    fn the_layer_sits_after_the_files_and_before_env() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("settings.yaml");
        std::fs::write(&file, "a: file\nb: file\nc: file\n").unwrap();
        let (dict, _) = mapped(&[("B", text("managed")), ("C", text("managed"))]).unwrap();
        let layer = ManagedLayer::new(r"HKLM\SOFTWARE\Policies\Example\App".into(), dict);
        temp_env::with_var("MANAGEDTEST_C", Some("env"), || {
            let options = super::super::ConfigOptions {
                env_prefix: "MANAGEDTEST".into(),
                config_paths: vec![file.clone()],
                search_standard_paths: false,
                load_dotenv: false,
                ..Default::default()
            };
            let config = super::super::Config::build(options, Some(&layer)).unwrap();
            assert_eq!(config.get_string("a").as_deref(), Some("file"));
            assert_eq!(config.get_string("b").as_deref(), Some("managed"));
            assert_eq!(config.get_string("c").as_deref(), Some("env"));
            let from = config.figment.find_metadata("b").unwrap();
            assert_eq!(from.name, r"managed: HKLM\SOFTWARE\Policies\Example\App");
        });
    }

    #[test]
    fn no_source_reads_nothing() {
        assert!(read(None).unwrap().is_none());
        assert!(read(Some(&ManagedSource::default())).unwrap().is_none());
    }

    /// Values, types and subkeys written under HKCU read back through the
    /// same code that reads HKLM's Policies key.
    #[cfg(windows)]
    #[test]
    fn registry_keys_read_into_the_tree() {
        use windows_registry::{CURRENT_USER, Type};

        let path = r"Software\ScaloManagedConfigTest";
        let _ = CURRENT_USER.remove_tree(path);
        let key = CURRENT_USER.create(path).unwrap();
        key.set_string("Name", "from-policy").unwrap();
        key.set_string("Brokers", "[a, b]").unwrap();
        key.set_u32("Port", 8080).unwrap();
        key.set_u64("Limit", u64::MAX).unwrap();
        key.set_multi_string("Tags", &["x", "y"]).unwrap();
        key.set_expand_string("Home", "%USERPROFILE%").unwrap();
        key.set_bytes("Blob", Type::Bytes, &[1, 2]).unwrap();
        CURRENT_USER
            .create(format!(r"{path}\Sink\Tls"))
            .unwrap()
            .set_string("CA_File", "c:\\ca.pem")
            .unwrap();
        let tree = registry_tree(CURRENT_USER, path);
        CURRENT_USER.remove_tree(path).unwrap();

        let mut out = Mapped::default();
        let dict = map(tree.unwrap().unwrap(), "", &mut out).unwrap();
        assert_eq!(
            as_json(dict),
            json!({"name": "from-policy", "brokers": ["a", "b"], "port": 8080, "limit": u64::MAX,
                   "tags": ["x", "y"], "sink": {"tls": {"ca_file": "c:\\ca.pem"}}})
        );
        out.skipped.sort();
        assert_eq!(
            out.skipped,
            [
                "blob: REG_BINARY is not a config type",
                "home: REG_EXPAND_SZ is not a config type"
            ]
        );
        assert!(
            registry_tree(CURRENT_USER, path).unwrap().is_none(),
            "a missing key is no store"
        );
    }

    /// `read` finds the app's key under HKLM's Policies hive and names it.
    #[cfg(windows)]
    #[test]
    #[ignore = "writes HKLM, which needs an administrator"]
    fn read_finds_the_policies_key() {
        let windows_key = "ScaloManagedConfigTest";
        let path = format!(r"SOFTWARE\Policies\{windows_key}");
        let hklm = windows_registry::LOCAL_MACHINE;
        hklm.create(&path)
            .unwrap()
            .set_string("Name", "from-policy")
            .unwrap();
        let source = ManagedSource {
            windows_key: windows_key.into(),
            ..ManagedSource::default()
        };
        let layer = read(Some(&source));
        hklm.remove_tree(&path).unwrap();

        let figment = figment::Figment::from(layer.unwrap().unwrap());
        assert_eq!(
            figment.extract_inner::<String>("name").unwrap(),
            "from-policy"
        );
        let from = figment.find_metadata("name").unwrap();
        assert_eq!(from.name, format!(r"managed: HKLM\{path}"));
    }

    /// A plist owned by the expected user and written by no one else reads,
    /// and any other owner or mode is skipped.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_plist_reads_only_with_a_safe_owner_and_mode() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("com.example.app.plist");
        let mut tls = plist::Dictionary::new();
        tls.insert("CA_File".into(), "/ca.pem".into());
        let mut root = plist::Dictionary::new();
        root.insert("Name".into(), "from-mdm".into());
        root.insert("Port".into(), 8080_i64.into());
        root.insert("Ratio".into(), 0.5_f64.into());
        root.insert("Enabled".into(), true.into());
        root.insert(
            "Brokers".into(),
            plist::Value::Array(vec!["a".into(), "b".into()]),
        );
        root.insert("Blob".into(), plist::Value::Data(vec![1]));
        root.insert("Sink".into(), plist::Value::Dictionary(tls));
        plist::Value::Dictionary(root)
            .to_file_binary(&path)
            .unwrap();
        let mode =
            |bits| std::fs::set_permissions(&path, std::fs::Permissions::from_mode(bits)).unwrap();
        mode(0o644);
        let me = std::fs::metadata(&path).unwrap().uid();

        let mut out = Mapped::default();
        let dict = map(plist_tree(&path, me).unwrap().unwrap(), "", &mut out).unwrap();
        assert_eq!(
            as_json(dict),
            json!({"name": "from-mdm", "port": 8080, "ratio": 0.5, "enabled": true,
                   "brokers": ["a", "b"], "sink": {"ca_file": "/ca.pem"}})
        );
        assert_eq!(out.skipped, ["blob: data is not a config type"]);

        assert!(
            plist_tree(&path, me + 1).unwrap().is_none(),
            "another owner is skipped"
        );
        mode(0o664);
        assert!(
            plist_tree(&path, me).unwrap().is_none(),
            "a group-writable file is skipped"
        );
        assert!(
            plist_tree(&dir.path().join("none.plist"), me)
                .unwrap()
                .is_none()
        );
    }
}
