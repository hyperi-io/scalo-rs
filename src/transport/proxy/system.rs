// Project:   scalo
// File:      src/transport/proxy/system.rs
// Purpose:   Read the machine's proxy setting: environment, WinHTTP, System Configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The machine's proxy for `proxy: system`.
//!
//! The environment comes first, as curl reads it. An entry it leaves empty is
//! filled from the OS store: the machine WinHTTP proxy on Windows and the
//! System Configuration proxies on macOS. Other systems have the environment
//! alone.

/// Raw proxy entries. An empty string is no proxy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Settings {
    /// Proxy for plain targets.
    pub http: String,
    /// Proxy for TLS targets.
    pub https: String,
    /// `NO_PROXY` rules, comma-separated.
    pub no: String,
    /// Hosts without a dot bypass the proxy.
    pub bypass_simple: bool,
}

impl Settings {
    /// Fill each empty entry from `other`.
    fn or(mut self, other: Self) -> Self {
        if self.http.is_empty() {
            self.http = other.http;
        }
        if self.https.is_empty() {
            self.https = other.https;
        }
        if self.no.is_empty() {
            self.no = other.no;
            self.bypass_simple |= other.bypass_simple;
        }
        self
    }
}

/// The environment, then the OS store.
pub(super) fn settings() -> Settings {
    let env = from_env();
    match os_settings() {
        Some(os) => env.or(os),
        None => env,
    }
}

/// `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY`, upper case first.
///
/// Under CGI the environment names nothing: a request's `Proxy:` header
/// arrives as `HTTP_PROXY` there.
pub(super) fn from_env() -> Settings {
    if std::env::var_os("REQUEST_METHOD").is_some() {
        return Settings::default();
    }
    let first = |names: [&str; 2]| {
        names
            .iter()
            .find_map(|name| std::env::var(name).ok().filter(|v| !v.trim().is_empty()))
            .unwrap_or_default()
    };
    let all = first(["ALL_PROXY", "all_proxy"]);
    let or_all = |value: String| if value.is_empty() { all.clone() } else { value };
    Settings {
        http: or_all(first(["HTTP_PROXY", "http_proxy"])),
        https: or_all(first(["HTTPS_PROXY", "https_proxy"])),
        no: first(["NO_PROXY", "no_proxy"]),
        bypass_simple: false,
    }
}

/// A WinHTTP proxy and bypass list as `netsh winhttp` stores them.
///
/// The proxy is `host:port` for every scheme, or `http=host:port;https=host:port`.
/// Bypass entries are split by `;` or white space. `<local>` is any host
/// without a dot, `*.example.com` becomes `.example.com`, and `10.*` becomes
/// `10.0.0.0/8`.
#[cfg(any(windows, test))]
pub(super) fn from_winhttp(proxy: &str, bypass: &str) -> Settings {
    let mut settings = Settings::default();
    for entry in proxy
        .split([';', ' '])
        .map(str::trim)
        .filter(|e| !e.is_empty())
    {
        match entry.split_once('=') {
            Some((scheme, value)) if scheme.eq_ignore_ascii_case("http") => {
                settings.http = value.trim().to_string();
            }
            Some((scheme, value)) if scheme.eq_ignore_ascii_case("https") => {
                settings.https = value.trim().to_string();
            }
            Some(_) => {}
            None => {
                settings.http = entry.to_string();
                settings.https = entry.to_string();
            }
        }
    }
    let mut no = Vec::new();
    for entry in bypass.split([';', ' ', '\t', '\r', '\n']).map(str::trim) {
        match entry {
            "" => {}
            "<local>" => settings.bypass_simple = true,
            _ => no.push(wildcard_to_rule(entry)),
        }
    }
    settings.no = no.join(",");
    settings
}

/// One WinHTTP or macOS bypass entry as a `NO_PROXY` rule.
#[cfg(any(windows, target_os = "macos", test))]
pub(super) fn wildcard_to_rule(entry: &str) -> String {
    if entry == "*" {
        return entry.to_string();
    }
    let parts: Vec<&str> = entry.split('.').collect();
    if let Some(at) = parts.iter().position(|p| *p == "*")
        && (1..=3).contains(&at)
        && parts.len() <= 4
        && parts[at..].iter().all(|p| *p == "*")
    {
        let mut octets = [0_u8; 4];
        let mut numeric = true;
        for (slot, part) in octets.iter_mut().zip(&parts[..at]) {
            match part.parse() {
                Ok(octet) => *slot = octet,
                Err(_) => numeric = false,
            }
        }
        if numeric {
            let [a, b, c, d] = octets;
            return format!("{a}.{b}.{c}.{d}/{}", at * 8);
        }
    }
    entry.trim_start_matches('*').to_string()
}

#[cfg(not(any(windows, target_os = "macos")))]
fn os_settings() -> Option<Settings> {
    None
}

/// The machine WinHTTP proxy, which `netsh winhttp set proxy` writes. A
/// service has no user proxy, so the per-user Internet Settings are not read.
#[cfg(windows)]
#[allow(unsafe_code, reason = "WinHTTP is a C API with no safe wrapper")]
fn os_settings() -> Option<Settings> {
    use windows::Win32::Foundation::{GlobalFree, HGLOBAL};
    use windows::Win32::Networking::WinHttp::{
        WINHTTP_ACCESS_TYPE_NAMED_PROXY, WINHTTP_PROXY_INFO, WinHttpGetDefaultProxyConfiguration,
    };
    use windows::core::PWSTR;

    /// Copy out and free a string WinHTTP allocated.
    fn take(text: PWSTR) -> String {
        if text.is_null() {
            return String::new();
        }
        // SAFETY: WinHTTP returned a NUL-terminated wide string it allocated with
        // GlobalAlloc. It is read once here and freed straight after.
        let copied = unsafe { text.to_string() }.unwrap_or_default();
        // SAFETY: the pointer came from GlobalAlloc and is not used again.
        let _ = unsafe { GlobalFree(Some(HGLOBAL(text.0.cast()))) };
        copied
    }

    let mut info = WINHTTP_PROXY_INFO::default();
    // SAFETY: `info` is a valid, writable WINHTTP_PROXY_INFO for the call.
    if let Err(error) = unsafe { WinHttpGetDefaultProxyConfiguration(&raw mut info) } {
        tracing::warn!(%error, "proxy: system: the WinHTTP proxy could not be read");
        return None;
    }
    let proxy = take(info.lpszProxy);
    let bypass = take(info.lpszProxyBypass);
    (info.dwAccessType == WINHTTP_ACCESS_TYPE_NAMED_PROXY).then(|| from_winhttp(&proxy, &bypass))
}

/// The System Configuration proxies, as System Settings > Network sets them.
#[cfg(target_os = "macos")]
fn os_settings() -> Option<Settings> {
    use system_configuration::core_foundation::array::CFArray;
    use system_configuration::core_foundation::base::{CFType, TCFType};
    use system_configuration::core_foundation::dictionary::CFDictionary;
    use system_configuration::core_foundation::number::CFNumber;
    use system_configuration::core_foundation::string::CFString;
    use system_configuration::dynamic_store::SCDynamicStoreBuilder;

    fn value(proxies: &CFDictionary<CFString, CFType>, key: &'static str) -> Option<CFType> {
        proxies
            .find(CFString::from_static_string(key))
            .map(|item| (*item).clone())
    }
    fn number(proxies: &CFDictionary<CFString, CFType>, key: &'static str) -> Option<i64> {
        value(proxies, key)?.downcast::<CFNumber>()?.to_i64()
    }
    fn text(proxies: &CFDictionary<CFString, CFType>, key: &'static str) -> Option<String> {
        Some(value(proxies, key)?.downcast::<CFString>()?.to_string())
    }
    fn entry(
        proxies: &CFDictionary<CFString, CFType>,
        enable: &'static str,
        host: &'static str,
        port: &'static str,
    ) -> String {
        if number(proxies, enable) != Some(1) {
            return String::new();
        }
        match (text(proxies, host), number(proxies, port)) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host,
            (None, _) => String::new(),
        }
    }
    #[allow(unsafe_code, reason = "CFArray hands back untyped item pointers")]
    fn exceptions(proxies: &CFDictionary<CFString, CFType>) -> Vec<String> {
        let Some(list) = value(proxies, "ExceptionsList").and_then(|v| v.downcast::<CFArray>())
        else {
            return Vec::new();
        };
        list.iter()
            .filter_map(|item| {
                // SAFETY: each item is a live CFTypeRef the array holds, and
                // the get rule takes its own retain.
                let item = unsafe { CFType::wrap_under_get_rule(*item) };
                item.downcast::<CFString>().map(|s| s.to_string())
            })
            .collect()
    }

    let store = SCDynamicStoreBuilder::new("scalo").build()?;
    let proxies = store.get_proxies()?;
    Some(Settings {
        http: entry(&proxies, "HTTPEnable", "HTTPProxy", "HTTPPort"),
        https: entry(&proxies, "HTTPSEnable", "HTTPSProxy", "HTTPSPort"),
        no: exceptions(&proxies)
            .iter()
            .map(|e| wildcard_to_rule(e))
            .collect::<Vec<String>>()
            .join(","),
        bypass_simple: number(&proxies, "ExcludeSimpleHostnames") == Some(1),
    })
}
