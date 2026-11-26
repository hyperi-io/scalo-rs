# WebAssembly plugins

The `wasm` feature hosts WebAssembly component plugins on wasmtime, held to its 48 LTS line. A plugin is a `.wasm` built for a WIT world the app defines. scalo loads it, checks who signed it, and runs every call under hard limits. It is off by default and not in `full`, because it adds a compiler to the build.

## Hosting a world

The world's crate runs `bindgen!` against scalo's copy of wasmtime and wraps the output in a `World`:

```rust,ignore
use scalo::wasm::wasmtime::component::{bindgen, HasSelf, InstancePre, Linker};
use scalo::wasm::wasmtime::{self, Store};
use scalo::wasm::{HostCtx, World};

bindgen!({ path: "wit/enrich.wit", wasmtime_crate: scalo::wasm::wasmtime });

struct EnrichWorld;

impl World for EnrichWorld {
    type Data = ();
    type Pre = EnrichPre<HostCtx<()>>;
    type Bindings = Enrich;
    fn link(l: &mut Linker<HostCtx<()>>) -> wasmtime::Result<()> {
        Enrich::add_to_linker::<_, HasSelf<HostCtx<()>>>(l, |ctx| ctx)
    }
    fn pre(p: InstancePre<HostCtx<()>>) -> wasmtime::Result<Self::Pre> { EnrichPre::new(p) }
    fn instantiate(p: &Self::Pre, s: &mut Store<HostCtx<()>>) -> wasmtime::Result<Enrich> {
        p.instantiate(s)
    }
}
```

One `WasmEngine` per process, one `Plugin` per loaded `.wasm`, one `Instance` per worker:

```rust,ignore
let engine = WasmEngine::new(WasmConfig::default())?;
let plugin = engine.load::<EnrichWorld>(PluginSource::Path(path), &policy)?;
let mut instance = plugin.instance(())?;
let out = instance.call(|bindings, store| bindings.call_enrich(store, &record))?;
```

`load` compiles, so run it on a blocking thread. `call` is sync. An import that hands the guest data charges `ctx.budget_mut().charge(bytes)` first. Guest state can vanish at any call, so a plugin keeps nothing it can't rebuild.

## Limits

Every call runs under these, and hitting one traps the guest, never the host. The trap replaces the guest before the next call.

| `WasmConfig` key | Default | On hit, `outcome` |
|---|---|---|
| `call_timeout_ms` | 1,000, in 1 ms epoch ticks | `timeout` |
| `max_memory_bytes` | 128 MiB per memory | `memory` |
| `max_table_elements` | 10,000 | `table` |
| `max_stack_bytes` | 512 KiB | `trap` (stack overflow) |
| `max_host_calls`, `max_host_bytes` | 100,000 calls, 64 MiB per call | `budget` |
| `max_instances` | 256 live instances, sized workers x plugins x 2 | `capacity` |
| `max_module_bytes` | 64 MiB | load refused, `too_large` |

`recycle_after_calls` (100,000) and `recycle_above_memory_bytes` (64 MiB) replace an instance that has served that many calls or grown past that much memory. `unhealthy_after_traps` (5) traps in a row mark the plugin unhealthy: every call then returns `CallError::Unhealthy` until the plugin is loaded again. The caller owns retry and dead-lettering.

WASI is linked over a context that grants nothing: no files (no preopened directories), no sockets, no environment, no arguments, no stdin. A guest's stdout and stderr become `info` log lines with `plugin` and `stream` fields, cut at 1 KiB and capped at 100 lines a second per instance. A `warn` line counts the rest. A plugin that imports `wasi:sockets` or `wasi:http` is refused at load. Secrets never enter the guest: an import resolves a credential name host-side.

## Loading and trust

`load` refuses, in order, before any guest code runs: a file over `max_module_bytes`, precompiled code (a `.cwasm`), bytes that aren't WebAssembly, a digest that isn't the pinned one, a signature no trusted signer made, an unsigned plugin, a forbidden import, and a plugin that needs more memories, tables or initial memory than the caps.

```yaml
allow_unsigned: false        # true loads unsigned plugins, warning every load
sha256: 3f1c...              # optional pin, 64 hex digits
signers:
  - kind: keyless            # CI: name.wasm.sigstore.json beside the file
    identity: https://github.com/acme/plugins/.github/workflows/release.yml@refs/heads/main
    issuer: https://token.actions.githubusercontent.com
  - kind: key                # operator: name.wasm.sig beside the file
    public_key: |
      -----BEGIN PUBLIC KEY-----
      ...
```

- Keyless: `cosign sign-blob --bundle enrich.wasm.sigstore.json enrich.wasm` in CI. The certificate's identity and issuer must match, and it chains to the Sigstore public-good root built into scalo, or to the `trusted_root.json` at `trusted_root_path`. Verification is offline.
- Key: `cosign sign-blob --key cosign.key --output-signature enrich.wasm.sig enrich.wasm`. P-256 and Ed25519 keys, no network. A key-signed Sigstore bundle with no transparency log entry doesn't verify, so key signing uses the detached signature.

The same `.wasm` loaded twice compiles once. `PluginSlot` swaps a plugin under load: each instance taken from the slot moves to the new plugin at its next call, keeping its host data, so a reload drops no call.

## Metrics

Bare names, labelled by `plugin`.

| Metric | Type | Labels |
|---|---|---|
| `wasm_plugin_calls_total` | counter | `plugin`, `outcome`: `ok`, `trap`, `timeout`, `memory`, `table`, `budget`, `capacity`, `unhealthy`, `poisoned` |
| `wasm_plugin_call_duration_seconds` | histogram | `plugin` |
| `wasm_plugin_loads_total` | counter | `plugin`, `outcome`: `ok`, `unsigned`, `bad_signature`, `untrusted_signer`, `digest_mismatch`, `precompiled`, `not_wasm`, `too_large`, `forbidden_import`, `over_limits`, `compile_error`, `bad_policy`, `read_error` |
| `wasm_plugin_compile_duration_seconds` | histogram | `plugin` |
| `wasm_plugin_instantiations_total` | counter | `plugin` |
| `wasm_plugin_memory_bytes` | gauge | `plugin`, the high-water mark since the plugin was loaded |
| `wasm_plugin_info` | gauge, 1 while a plugin with these labels is loaded, then 0 | `plugin`, `digest`, `signer` |
| `wasm_pool_component_instances`, `wasm_pool_core_instances`, `wasm_pool_memories`, `wasm_pool_tables` | gauge | none, updated each second |

## Weight

`wasm` pulls wasmtime and Cranelift, `wasmtime-wasi` (whose `p2` compiles wasmtime's async support, which the sync calls never use), `sigstore-verify` pinned exactly at 0.17.0 with no TLS or TUF, `sha2` and `arc-swap`. It adds second copies of `jiff` 0.2 and the RustCrypto 0.10 line (`digest`, `const-oid`, `block-buffer`) beside scalo's own, through sigstore's X.509 stack. `wasm-test-support` adds `wat`, for WAT test guests, and is never in a shipping build.
