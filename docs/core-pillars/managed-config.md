# Managed config

An administrator can manage an app's settings centrally through Group Policy or Intune on Windows, and Jamf or another MDM on macOS. The app opts in by naming its store, and scalo reads it as one more config layer. Linux has no managed layer and stays file-driven.

Precedence, highest first: CLI args, env vars and `.env`, **managed**, `settings.{env}.yaml`, `settings.yaml` (and the file `--config` or `load_typed` names), `defaults.yaml`, hard-coded defaults. Policy beats a local file edit, and an admin on the box can still override it with an env var or a flag.

## Opting in

```rust
use scalo::config::{ConfigOptions, ManagedSource};

let source = ManagedSource {
    windows_key: r"Example\App".into(),     // HKLM\SOFTWARE\Policies\Example\App
    macos_domain: "com.example.app".into(),  // /Library/Managed Preferences/com.example.app.plist
};

// The cascade:
scalo::config::setup(ConfigOptions { managed: Some(source.clone()), ..Default::default() })?;
// Or an app's whole config, as load_typed reads it:
let config: AppConfig = scalo::config::load_typed_managed(path, "EXAMPLE", &source)?;
```

A `ServiceApp` returns the source from `managed_source()`, so `run_app` puts the layer in the cascade, and its `load_config` calls `load_typed_managed`.

Without a `ManagedSource` nothing is read, and an empty field reads nothing on its OS. The store is read at every load, so a reload that calls `load_typed_managed` again picks up a policy change. Nothing watches the store, and nothing is ever written back.

## Windows: Group Policy

scalo reads `HKLM\SOFTWARE\Policies\<windows_key>`. Administrators and SYSTEM alone can write there, and Group Policy removes its values when the policy is unlinked. A subkey is a section, so `...\App\Sink\Tls` is `sink.tls`. Names are lowercased, and `__` in a value name nests as it does in an env var.

| Registry type | Reads as |
| --- | --- |
| `REG_SZ` | What the same text means in an env var: `warn`, `8080`, `true`, `[a, b]`, `[{url="https://a"}]` |
| `REG_DWORD`, `REG_QWORD` | An integer |
| `REG_MULTI_SZ` | A list of strings, each taken as written |
| Anything else (`REG_EXPAND_SZ`, `REG_BINARY`, ...) | Skipped with a warning naming the value |

An ADMX template writes these values from a policy. The same settings by hand:

```text
reg add "HKLM\SOFTWARE\Policies\Example\App" /v log_level /t REG_SZ /d warn /f
reg add "HKLM\SOFTWARE\Policies\Example\App\Sink" /v endpoint /t REG_SZ /d https://collector.example.com /f
reg add "HKLM\SOFTWARE\Policies\Example\App\Sink" /v batch_size /t REG_DWORD /d 500 /f
reg add "HKLM\SOFTWARE\Policies\Example\App\Sink\Tls" /v ca_file /t REG_SZ /d C:\ProgramData\Example\ca.pem /f
```

## macOS: Jamf and other MDMs

scalo reads `/Library/Managed Preferences/<macos_domain>.plist`, the machine-level file a configuration profile writes. A file root doesn't own, or a group or other user can write, is skipped with a warning. The per-user managed preferences folders are never read. A dict is a section, an array a list, and a string, integer, real or bool its scalar. Names are lowercased as on Windows.

In Jamf Pro, add an Application & Custom Settings payload to a computer-level profile, with the preference domain `com.example.app` and this property list:

```xml
<dict>
  <key>log_level</key><string>warn</string>
  <key>sink</key>
  <dict>
    <key>endpoint</key><string>https://collector.example.com</string>
    <key>batch_size</key><integer>500</integer>
    <key>tls</key>
    <dict>
      <key>ca_file</key><string>/Library/Application Support/Example/ca.pem</string>
    </dict>
  </dict>
</dict>
```

## Secrets never come from policy

Group Policy's `Registry.pol` sits in SYSVOL, where every domain user can read it. So the load fails, naming the key, when a key's last segment is `password`, `passwd`, `secret`, `token`, `api_key`, `apikey`, `private_key`, `credential` or `credentials`, or ends `_` and one of them: `kafka.sasl_password`, `client_secret`, `auth.token`. Case doesn't matter. `auth.mode`, `max_tokens` and `partition_key` are allowed. Put the secret in a file the service alone can read and set the key's file-path form instead: a name ending `_file` or `_path`, such as `tls.key_file`, is allowed.

## What it logs

INFO once per read with the store and the number of keys, DEBUG for each key name (never a value), WARN for a skipped value or an unsafe plist. Lines logged before `logger::setup` are held and emitted once it runs, as `load_typed`'s are.

figment names the layer `managed: <store>`, so an error about a value the layer set ends `in managed: HKLM\SOFTWARE\Policies\Example\App`.
