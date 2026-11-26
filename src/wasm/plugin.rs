// Project:   scalo
// File:      src/wasm/plugin.rs
// Purpose:   Loaded plugins, their instances and the call wrapper
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Loaded plugins, their per-worker instances and the call wrapper.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use wasmtime::component::{InstancePre, Linker};
use wasmtime::{PoolConcurrencyLimitError, Store, Trap};

use super::engine::EngineInner;
use super::host::{HostCtx, LimitHit};

/// A WIT world scalo can host, wrapping what `wasmtime::component::bindgen!`
/// generates for it.
///
/// The world's crate runs `bindgen!` with `wasmtime_crate:
/// scalo::wasm::wasmtime`, so both sides build one wasmtime.
pub trait World: 'static {
    /// Host data each instance keeps across calls.
    type Data: Send + 'static;
    /// bindgen's `XPre`.
    type Pre: Clone + Send + Sync + 'static;
    /// bindgen's `X`.
    type Bindings: Send + 'static;

    /// Add the world's imports. WASI is linked before this runs.
    fn link(linker: &mut Linker<HostCtx<Self::Data>>) -> wasmtime::Result<()>;

    /// bindgen's `XPre::new`.
    fn pre(pre: InstancePre<HostCtx<Self::Data>>) -> wasmtime::Result<Self::Pre>;

    /// bindgen's `XPre::instantiate`.
    fn instantiate(
        pre: &Self::Pre,
        store: &mut Store<HostCtx<Self::Data>>,
    ) -> wasmtime::Result<Self::Bindings>;
}

/// Why a call returned no result.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CallError {
    /// The call ran past `call_timeout_ms`.
    #[error("call ran past its deadline")]
    Timeout,
    /// The guest ran into a cap.
    #[error("guest ran into its {0}")]
    Limit(LimitHit),
    /// The guest trapped.
    #[error("guest trapped: {0}")]
    Trap(Trap),
    /// Every pooled instance slot is in use. Back off and retry.
    #[error("no pooled instance slot is free")]
    Capacity,
    /// The plugin trapped this many times in a row and takes no more calls
    /// until it is loaded again.
    #[error("plugin is unhealthy after {0} traps in a row")]
    Unhealthy(u32),
    /// A previous call panicked inside its closure, so this instance is gone.
    #[error("instance was lost to a panic in an earlier call")]
    Poisoned,
    /// Any other failure, from instantiation or a host import.
    #[error("call failed: {0:#}")]
    Other(wasmtime::Error),
}

impl CallError {
    fn from_wasmtime(error: wasmtime::Error) -> Self {
        if let Some(limit) = error.downcast_ref::<LimitHit>() {
            return Self::Limit(*limit);
        }
        if error.is::<PoolConcurrencyLimitError>() {
            return Self::Capacity;
        }
        match error.downcast_ref::<Trap>() {
            Some(Trap::Interrupt) => Self::Timeout,
            Some(trap) => Self::Trap(*trap),
            None => Self::Other(error),
        }
    }

    /// The `outcome` label this error counts under.
    #[must_use]
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Limit(LimitHit::Memory) => "memory",
            Self::Limit(LimitHit::Table) => "table",
            Self::Limit(LimitHit::Budget) => "budget",
            Self::Trap(_) | Self::Other(_) => "trap",
            Self::Capacity => "capacity",
            Self::Unhealthy(_) => "unhealthy",
            Self::Poisoned => "poisoned",
        }
    }
}

/// Traps in a row, against the limit that marks the plugin unhealthy.
#[derive(Debug)]
struct Health {
    traps_in_a_row: AtomicU32,
    limit: u32,
}

impl Health {
    fn check(&self) -> Result<(), CallError> {
        if self.limit > 0 && self.traps_in_a_row.load(Ordering::Relaxed) >= self.limit {
            return Err(CallError::Unhealthy(self.limit));
        }
        Ok(())
    }

    fn record(&self, trapped: bool) {
        if trapped {
            self.traps_in_a_row.fetch_add(1, Ordering::Relaxed);
        } else if self.traps_in_a_row.load(Ordering::Relaxed) < self.limit {
            self.traps_in_a_row.store(0, Ordering::Relaxed);
        }
    }
}

/// A compiled, linked plugin. Cheap to share: each worker takes its own
/// [`Instance`] from it.
pub struct Plugin<W: World> {
    name: Arc<str>,
    digest: [u8; 32],
    pre: W::Pre,
    engine: Arc<EngineInner>,
    health: Health,
}

impl<W: World> Plugin<W> {
    pub(super) fn new(
        name: Arc<str>,
        digest: [u8; 32],
        pre: W::Pre,
        engine: Arc<EngineInner>,
    ) -> Self {
        let limit = engine.config.unhealthy_after_traps;
        Self {
            name,
            digest,
            pre,
            engine,
            health: Health {
                traps_in_a_row: AtomicU32::new(0),
                limit,
            },
        }
    }

    /// The plugin's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The sha256 of the `.wasm` it was loaded from.
    #[must_use]
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// `false` once the plugin has trapped `unhealthy_after_traps` times in a
    /// row. It stays so until it is loaded again.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.health.check().is_ok()
    }

    /// An instance for one worker, holding `data` across calls.
    ///
    /// It instantiates now, so a full pool shows here as
    /// [`CallError::Capacity`]. Guest state may vanish at any call: a trap, the
    /// recycle count or the memory mark replace the instance, and `data` moves
    /// to the new one.
    pub fn instance(self: &Arc<Self>, data: W::Data) -> Result<Instance<W>, CallError> {
        self.health.check()?;
        let mut store = self.new_store(data);
        match W::instantiate(&self.pre, &mut store) {
            Ok(bindings) => Ok(Instance {
                plugin: Arc::clone(self),
                state: State::Ready { store, bindings },
                calls: 0,
            }),
            Err(error) => Err(CallError::from_wasmtime(error)),
        }
    }

    fn new_store(&self, data: W::Data) -> Store<HostCtx<W::Data>> {
        let ctx = HostCtx::new(data, Arc::clone(&self.name), &self.engine.config);
        let mut store = Store::new(&self.engine.engine, ctx);
        store.limiter(|ctx| ctx.limiter_mut());
        store.epoch_deadline_trap();
        store.set_epoch_deadline(self.engine.deadline_ticks);
        store
    }

    /// A fresh store holding `old`'s data. Dropping `old` frees its pool slot.
    fn renew(&self, old: Store<HostCtx<W::Data>>) -> Store<HostCtx<W::Data>> {
        self.new_store(old.into_data().into_data())
    }
}

enum State<W: World> {
    Ready {
        store: Store<HostCtx<W::Data>>,
        bindings: W::Bindings,
    },
    /// A store with no guest in it yet, after a trap or a recycle.
    Idle(Store<HostCtx<W::Data>>),
    /// Taken by a call whose closure panicked.
    Poisoned,
}

/// One worker's instance of a [`Plugin`]. Not shared: a call needs `&mut`.
pub struct Instance<W: World> {
    plugin: Arc<Plugin<W>>,
    state: State<W>,
    calls: u64,
}

impl<W: World> Instance<W> {
    /// Call into the guest through `f`, which gets bindgen's bindings and the
    /// store.
    ///
    /// The call gets `call_timeout_ms` of CPU and a fresh import budget. A
    /// trap replaces the instance before the next call and counts toward
    /// `unhealthy_after_traps`. The caller owns any retry or dead-lettering.
    pub fn call<R>(
        &mut self,
        f: impl FnOnce(&W::Bindings, &mut Store<HostCtx<W::Data>>) -> wasmtime::Result<R>,
    ) -> Result<R, CallError> {
        self.plugin.health.check()?;
        let deadline = self.plugin.engine.deadline_ticks;
        let (mut store, bindings) = match std::mem::replace(&mut self.state, State::Poisoned) {
            State::Ready { store, bindings } => (store, bindings),
            State::Idle(mut store) => {
                store.set_epoch_deadline(deadline);
                match W::instantiate(&self.plugin.pre, &mut store) {
                    Ok(bindings) => (store, bindings),
                    Err(error) => {
                        self.state = State::Idle(self.plugin.renew(store));
                        return Err(CallError::from_wasmtime(error));
                    }
                }
            }
            State::Poisoned => return Err(CallError::Poisoned),
        };

        store.set_epoch_deadline(deadline);
        store.data_mut().budget_mut().reset();
        let result = f(&bindings, &mut store);
        self.calls = self.calls.saturating_add(1);

        match result {
            Ok(value) => {
                self.plugin.health.record(false);
                if self.due_for_recycle(&store) {
                    self.calls = 0;
                    self.state = State::Idle(self.plugin.renew(store));
                } else {
                    self.state = State::Ready { store, bindings };
                }
                Ok(value)
            }
            Err(error) => {
                self.plugin.health.record(true);
                self.calls = 0;
                self.state = State::Idle(self.plugin.renew(store));
                Err(CallError::from_wasmtime(error))
            }
        }
    }

    /// The plugin this instance runs.
    #[must_use]
    pub fn plugin(&self) -> &Arc<Plugin<W>> {
        &self.plugin
    }

    fn due_for_recycle(&self, store: &Store<HostCtx<W::Data>>) -> bool {
        let config = &self.plugin.engine.config;
        let by_count = config.recycle_after_calls > 0 && self.calls >= config.recycle_after_calls;
        let by_memory = config.recycle_above_memory_bytes > 0
            && store.data().peak_memory_bytes() > config.recycle_above_memory_bytes;
        by_count || by_memory
    }
}
