// Project:   scalo
// File:      src/wasm/engine.rs
// Purpose:   The process-wide wasm engine, its epoch ticker and the loader
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The process-wide engine and the plugin loader.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use sigstore_verify::trust_root::TrustedRoot;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig};

use super::config::WasmConfig;
use super::host::HostCtx;
use super::metrics;
use super::plugin::{Plugin, World};
use super::trust::{self, Evidence, Refusal, TrustPolicy};

/// The keyless bundle beside a `.wasm` file: `name.wasm.sigstore.json`.
const BUNDLE_SUFFIX: &str = ".sigstore.json";

/// The key signature beside a `.wasm` file: `name.wasm.sig`.
const SIGNATURE_SUFFIX: &str = ".sig";

/// The `signer` a plugin loaded without a signature reports.
pub(super) const UNSIGNED: &str = "unsigned";

/// How often the ticker advances the epoch, and so the deadline's resolution.
const EPOCH_TICK: Duration = Duration::from_millis(1);

/// Ticks between pool gauge updates: about once a second.
const POOL_METRICS_EVERY: u32 = 1_000;

/// Pool slots one plugin instance may use, a component holding several core
/// instances, memories and tables.
const CORE_INSTANCES_PER_COMPONENT: u32 = 16;
const MEMORIES_PER_COMPONENT: u32 = 2;
const TABLES_PER_COMPONENT: u32 = 4;

/// Import namespaces no plugin may use. HTTP isn't linked, and a sockets
/// import would link to a policy that refuses every address.
const FORBIDDEN_IMPORTS: [&str; 2] = ["wasi:sockets/", "wasi:http/"];

const WASM_MAGIC: &[u8; 4] = b"\0asm";

/// Why an engine would not start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// wasmtime rejected the configuration.
    #[error("wasm engine config: {0:#}")]
    Config(wasmtime::Error),
    /// The epoch ticker thread would not start.
    #[error("wasm epoch ticker: {0}")]
    Ticker(#[source] std::io::Error),
    /// The Sigstore trusted root would not load.
    #[error("wasm trusted root: {0}")]
    TrustedRoot(String),
}

/// Why a plugin did not load. Each variant is refused before any guest code
/// runs.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LoadError {
    /// The file could not be read.
    #[error("reading {path}: {source}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The read error.
        #[source]
        source: std::io::Error,
    },
    /// The file is over `max_module_bytes`.
    #[error("{name} is over the {limit}-byte cap")]
    TooLarge {
        /// The plugin.
        name: String,
        /// `max_module_bytes`.
        limit: usize,
    },
    /// The file is compiled machine code (a `.cwasm`), which never loads.
    #[error("{name} is precompiled code, and only .wasm loads")]
    Precompiled {
        /// The plugin.
        name: String,
    },
    /// The file is not WebAssembly.
    #[error("{name} is not a WebAssembly binary")]
    NotWasm {
        /// The plugin.
        name: String,
    },
    /// The file's sha256 is not the pinned one.
    #[error("{name} has sha256 {actual}, not the pinned {expected}")]
    DigestMismatch {
        /// The plugin.
        name: String,
        /// The pinned digest, hex.
        expected: String,
        /// The file's digest, hex.
        actual: String,
    },
    /// The plugin is unsigned and its policy doesn't allow that.
    #[error("{name} is unsigned, and its trust policy refuses unsigned plugins")]
    Unsigned {
        /// The plugin.
        name: String,
    },
    /// The signature bundle doesn't parse, or doesn't cover these bytes.
    #[error("{name} has a bad signature: {detail}")]
    BadSignature {
        /// The plugin.
        name: String,
        /// sigstore's reason.
        detail: String,
    },
    /// The signature holds up, but no signer in the policy made it.
    #[error("{name} is not signed by a signer its trust policy names: {detail}")]
    UntrustedSigner {
        /// The plugin.
        name: String,
        /// sigstore's reason for the last signer tried.
        detail: String,
    },
    /// The trust policy itself doesn't parse.
    #[error("{name} has a trust policy that doesn't parse: {detail}")]
    BadPolicy {
        /// The plugin.
        name: String,
        /// What is wrong with it.
        detail: String,
    },
    /// The plugin imports an interface plugins may not use.
    #[error("{name} imports {import}, which plugins may not use")]
    ForbiddenImport {
        /// The plugin.
        name: String,
        /// The import.
        import: String,
    },
    /// The plugin needs more memories, tables or initial memory than the
    /// engine's caps allow.
    #[error("{name} needs more than the engine's caps: {detail}")]
    OverLimits {
        /// The plugin.
        name: String,
        /// What it needs.
        detail: String,
    },
    /// The plugin does not compile, or its imports do not link.
    #[error("{name} does not compile or link: {detail}")]
    Compile {
        /// The plugin.
        name: String,
        /// wasmtime's error chain.
        detail: String,
    },
}

impl LoadError {
    /// The `outcome` label this refusal counts under.
    #[must_use]
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::Read { .. } => "read_error",
            Self::TooLarge { .. } => "too_large",
            Self::Precompiled { .. } => "precompiled",
            Self::NotWasm { .. } => "not_wasm",
            Self::DigestMismatch { .. } => "digest_mismatch",
            Self::Unsigned { .. } => "unsigned",
            Self::BadSignature { .. } => "bad_signature",
            Self::UntrustedSigner { .. } => "untrusted_signer",
            Self::BadPolicy { .. } => "bad_policy",
            Self::ForbiddenImport { .. } => "forbidden_import",
            Self::OverLimits { .. } => "over_limits",
            Self::Compile { .. } => "compile_error",
        }
    }
}

/// Where a plugin's `.wasm` and the proof of who signed it come from.
#[derive(Debug, Clone, Copy)]
pub enum PluginSource<'a> {
    /// A file, named after its stem. Its keyless bundle is the file beside it
    /// with `.sigstore.json` added, and its key signature the one with `.sig`
    /// added.
    Path(&'a Path),
    /// Bytes already in memory.
    Bytes {
        /// The plugin's name.
        name: &'a str,
        /// The `.wasm`.
        bytes: &'a [u8],
        /// A keyless Sigstore bundle, JSON.
        bundle: Option<&'a [u8]>,
        /// A key signature, base64.
        signature: Option<&'a [u8]>,
    },
}

/// Advances the engine's epoch every [`EPOCH_TICK`] until dropped.
struct Ticker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Ticker {
    fn start(engine: Engine) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("wasm-epoch".to_string())
            .spawn(move || {
                let mut ticks: u32 = 0;
                while !flag.load(Ordering::Relaxed) {
                    std::thread::sleep(EPOCH_TICK);
                    engine.increment_epoch();
                    ticks = ticks.wrapping_add(1);
                    if ticks.is_multiple_of(POOL_METRICS_EVERY) {
                        metrics::pool(&engine);
                    }
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            tracing::warn!("wasm: the epoch ticker thread panicked");
        }
    }
}

/// A compiled component and the live plugins built from it.
struct Compiled {
    component: Component,
    plugins: usize,
}

pub(super) struct EngineInner {
    pub(super) engine: Engine,
    pub(super) config: WasmConfig,
    pub(super) deadline_ticks: u64,
    trusted_root: TrustedRoot,
    components: Mutex<HashMap<[u8; 32], Compiled>>,
    _ticker: Ticker,
}

impl EngineInner {
    fn components(&self) -> MutexGuard<'_, HashMap<[u8; 32], Compiled>> {
        self.components
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The component for `digest`, compiled once while any plugin uses it.
    fn compile(&self, name: &str, digest: [u8; 32], bytes: &[u8]) -> Result<Component, LoadError> {
        if let Some(compiled) = self.components().get(&digest) {
            return Ok(compiled.component.clone());
        }
        let started = Instant::now();
        let component =
            Component::from_binary(&self.engine, bytes).map_err(|e| LoadError::Compile {
                name: name.to_string(),
                detail: format!("{e:#}"),
            })?;
        metrics::compiled(name, started.elapsed());
        let mut components = self.components();
        components.retain(|_, compiled| compiled.plugins > 0);
        components.entry(digest).or_insert(Compiled {
            component: component.clone(),
            plugins: 0,
        });
        Ok(component)
    }

    pub(super) fn retain(&self, digest: &[u8; 32]) {
        if let Some(compiled) = self.components().get_mut(digest) {
            compiled.plugins += 1;
        }
    }

    pub(super) fn release(&self, digest: &[u8; 32]) {
        let mut components = self.components();
        if let Some(compiled) = components.get_mut(digest) {
            compiled.plugins = compiled.plugins.saturating_sub(1);
            if compiled.plugins == 0 {
                components.remove(digest);
            }
        }
    }
}

/// One per process: the compiler, the instance pool and the epoch ticker
/// every plugin shares. Cheap to clone.
#[derive(Clone)]
pub struct WasmEngine {
    inner: Arc<EngineInner>,
}

impl WasmEngine {
    /// Start an engine with `config`'s caps.
    pub fn new(config: WasmConfig) -> Result<Self, EngineError> {
        let trusted_root = trust::trusted_root(config.trusted_root_path.as_deref())
            .map_err(EngineError::TrustedRoot)?;
        let engine = Engine::new(&engine_config(&config)).map_err(EngineError::Config)?;
        let ticker = Ticker::start(engine.clone()).map_err(EngineError::Ticker)?;
        // One tick more than the timeout, so a call never gets less than it.
        let deadline_ticks = config.call_timeout_ms.saturating_add(1);
        Ok(Self {
            inner: Arc::new(EngineInner {
                engine,
                config,
                deadline_ticks,
                trusted_root,
                components: Mutex::new(HashMap::new()),
                _ticker: ticker,
            }),
        })
    }

    /// The caps this engine runs plugins under.
    #[must_use]
    pub fn config(&self) -> &WasmConfig {
        &self.inner.config
    }

    /// Compiled components held for live plugins. A `.wasm` loaded twice
    /// compiles once.
    #[must_use]
    pub fn cached_components(&self) -> usize {
        self.inner
            .components()
            .values()
            .filter(|compiled| compiled.plugins > 0)
            .count()
    }

    /// Check, compile and link a plugin for world `W`.
    ///
    /// Refuses, in order: a file over `max_module_bytes`, precompiled code,
    /// anything that isn't WebAssembly, a digest that isn't the pinned one, a
    /// bad signature or one no trusted signer made, an unsigned plugin the
    /// policy doesn't allow, a forbidden import, and a plugin that needs more
    /// than the engine's caps. Compiling is CPU-bound, so call this from a
    /// blocking thread, never an async task.
    pub fn load<W: World>(
        &self,
        source: PluginSource<'_>,
        trust: &TrustPolicy,
    ) -> Result<Arc<Plugin<W>>, LoadError> {
        let name = match source {
            PluginSource::Path(path) => plugin_name(path),
            PluginSource::Bytes { name, .. } => name.to_string(),
        };
        let loaded = self.load_inner::<W>(source, trust);
        metrics::load(
            &name,
            loaded.as_ref().map_or_else(LoadError::outcome, |_| "ok"),
        );
        loaded
    }

    fn load_inner<W: World>(
        &self,
        source: PluginSource<'_>,
        trust: &TrustPolicy,
    ) -> Result<Arc<Plugin<W>>, LoadError> {
        let limit = self.inner.config.max_module_bytes;
        let read: Vec<u8>;
        let read_bundle: Option<Vec<u8>>;
        let read_signature: Option<Vec<u8>>;
        let (name, bytes, evidence): (String, &[u8], Evidence<'_>) = match source {
            PluginSource::Path(path) => {
                read = read_capped(path, limit)?;
                read_bundle = read_beside(path, BUNDLE_SUFFIX, limit)?;
                read_signature = read_beside(path, SIGNATURE_SUFFIX, limit)?;
                let evidence = Evidence {
                    bundle: read_bundle.as_deref(),
                    signature: read_signature.as_deref(),
                };
                (plugin_name(path), &read, evidence)
            }
            PluginSource::Bytes {
                name,
                bytes,
                bundle,
                signature,
            } => (name.to_string(), bytes, Evidence { bundle, signature }),
        };

        if bytes.len() > limit {
            return Err(LoadError::TooLarge { name, limit });
        }
        if Engine::detect_precompiled(bytes).is_some() {
            return Err(LoadError::Precompiled { name });
        }
        if !bytes.starts_with(WASM_MAGIC) {
            return Err(LoadError::NotWasm { name });
        }
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        if let Some(pinned) = &trust.sha256 {
            let Some(expected) = trust::parse_sha256(pinned) else {
                return Err(LoadError::BadPolicy {
                    name,
                    detail: format!("sha256 {pinned:?} is not 64 hex digits"),
                });
            };
            if expected != digest {
                return Err(LoadError::DigestMismatch {
                    name,
                    expected: hex(&expected),
                    actual: hex(&digest),
                });
            }
        }
        let signer = if evidence.is_empty() {
            if !trust.allow_unsigned {
                return Err(LoadError::Unsigned { name });
            }
            tracing::warn!(plugin = %name, sha256 = %hex(&digest), "wasm: loading an unsigned plugin, as its trust policy allows");
            UNSIGNED.to_string()
        } else {
            match trust::check(bytes, evidence, trust, &self.inner.trusted_root) {
                Ok(signer) => signer,
                Err(Refusal::Bad(detail)) => return Err(LoadError::BadSignature { name, detail }),
                Err(Refusal::Untrusted(detail)) => {
                    return Err(LoadError::UntrustedSigner { name, detail });
                }
            }
        };

        let engine = &self.inner.engine;
        let component = self.inner.compile(&name, digest, bytes)?;
        self.check_imports(&name, &component)?;
        self.check_resources(&name, &component)?;

        let mut linker: Linker<HostCtx<W::Data>> = Linker::new(engine);
        let pre = wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .and_then(|()| W::link(&mut linker))
            .and_then(|()| linker.instantiate_pre(&component))
            .and_then(W::pre)
            .map_err(|e| LoadError::Compile {
                name: name.clone(),
                detail: format!("{e:#}"),
            })?;
        Ok(Arc::new(Plugin::new(
            Arc::from(name),
            digest,
            signer,
            pre,
            Arc::clone(&self.inner),
        )))
    }

    fn check_imports(&self, name: &str, component: &Component) -> Result<(), LoadError> {
        let engine = &self.inner.engine;
        let ty = component.component_type();
        let forbidden = ty.imports(engine).map(|(import, _)| import).find(|import| {
            FORBIDDEN_IMPORTS
                .iter()
                .any(|prefix| import.starts_with(prefix))
        });
        match forbidden {
            Some(import) => Err(LoadError::ForbiddenImport {
                name: name.to_string(),
                import: import.to_string(),
            }),
            None => Ok(()),
        }
    }

    fn check_resources(&self, name: &str, component: &Component) -> Result<(), LoadError> {
        let Some(needs) = component.resources_required() else {
            return Ok(());
        };
        let config = &self.inner.config;
        let page = 64 * 1024_u64;
        let initial_memory = needs
            .max_initial_memory_size
            .unwrap_or(0)
            .saturating_mul(page);
        let initial_table = needs.max_initial_table_size.unwrap_or(0);
        let detail = if needs.num_memories > MEMORIES_PER_COMPONENT {
            Some(format!(
                "{} memories, cap {MEMORIES_PER_COMPONENT}",
                needs.num_memories
            ))
        } else if needs.num_tables > TABLES_PER_COMPONENT {
            Some(format!(
                "{} tables, cap {TABLES_PER_COMPONENT}",
                needs.num_tables
            ))
        } else if initial_memory > config.max_memory_bytes as u64 {
            Some(format!(
                "{initial_memory} bytes of initial memory, cap {}",
                config.max_memory_bytes
            ))
        } else if initial_table > config.max_table_elements as u64 {
            Some(format!(
                "{initial_table} initial table elements, cap {}",
                config.max_table_elements
            ))
        } else {
            None
        };
        match detail {
            Some(detail) => Err(LoadError::OverLimits {
                name: name.to_string(),
                detail,
            }),
            None => Ok(()),
        }
    }
}

fn engine_config(config: &WasmConfig) -> Config {
    let instances = config.max_instances;
    let mut pool = PoolingAllocationConfig::default();
    pool.total_component_instances(instances)
        .total_core_instances(instances.saturating_mul(CORE_INSTANCES_PER_COMPONENT))
        .total_memories(instances.saturating_mul(MEMORIES_PER_COMPONENT))
        .total_tables(instances.saturating_mul(TABLES_PER_COMPONENT))
        .max_core_instances_per_component(CORE_INSTANCES_PER_COMPONENT)
        .max_memories_per_component(MEMORIES_PER_COMPONENT)
        .max_tables_per_component(TABLES_PER_COMPONENT)
        .max_memory_size(config.max_memory_bytes)
        .table_elements(config.max_table_elements);

    let mut engine = Config::new();
    engine
        .epoch_interruption(true)
        .consume_fuel(false)
        .max_wasm_stack(config.max_stack_bytes)
        .wasm_memory64(false)
        .wasm_gc(false)
        .wasm_stack_switching(false)
        .allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
    engine
}

fn plugin_name(path: &Path) -> String {
    path.file_stem().map_or_else(
        || path.display().to_string(),
        |stem| stem.to_string_lossy().into_owned(),
    )
}

/// Read `path`, refusing past `limit` bytes without reading the rest.
fn read_capped(path: &Path, limit: usize) -> Result<Vec<u8>, LoadError> {
    let read_error = |source| LoadError::Read {
        path: path.to_path_buf(),
        source,
    };
    let file = std::fs::File::open(path).map_err(read_error)?;
    let mut bytes = Vec::new();
    let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    file.take(cap).read_to_end(&mut bytes).map_err(read_error)?;
    if bytes.len() > limit {
        return Err(LoadError::TooLarge {
            name: plugin_name(path),
            limit,
        });
    }
    Ok(bytes)
}

/// The file beside `path` with `suffix` added, or `None` when there is none.
fn read_beside(path: &Path, suffix: &str, limit: usize) -> Result<Option<Vec<u8>>, LoadError> {
    let mut beside = path.as_os_str().to_owned();
    beside.push(suffix);
    let beside = PathBuf::from(beside);
    if !beside.exists() {
        return Ok(None);
    }
    read_capped(&beside, limit).map(Some)
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| [DIGITS[usize::from(b >> 4)], DIGITS[usize::from(b & 0xf)]])
        .map(char::from)
        .collect()
}
