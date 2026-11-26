// Project:   scalo
// File:      src/wasm/tests.rs
// Purpose:   The wasm host's limits, loader and recycling against a fixture guest
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;

use sha2::Digest;
use wasmtime::component::{HasSelf, InstancePre, Linker};
use wasmtime::{Store, Trap};

use super::*;

mod fixture {
    wasmtime::component::bindgen!({
        inline: "
            package scalo:test;
            world fixture {
                import tick: func();
                export run: func(op: u32, n: u32) -> u32;
            }
        ",
        imports: { default: trappable },
    });
}

/// One guest, an op per limit: 0 returns `n`, 1 loops forever, 2 grows memory
/// by `n` pages, 3 grows the table by `n`, 4 recurses forever, 5 traps, 6 calls
/// the `tick` import `n` times, 7 counts calls in a global.
const GUEST: &str = r#"
(component
  (import "tick" (func $tick))
  (core func $tick_lowered (canon lower (func $tick)))
  (core module $m
    (import "host" "tick" (func $tick))
    (memory 1)
    (table 1 funcref)
    (global $count (mut i32) (i32.const 0))
    (func $recurse (param i32) (result i32)
      (call $recurse (i32.add (local.get 0) (i32.const 1))))
    (func (export "run") (param $op i32) (param $n i32) (result i32)
      (local $i i32)
      (if (i32.eq (local.get $op) (i32.const 0)) (then (return (local.get $n))))
      (if (i32.eq (local.get $op) (i32.const 1)) (then (loop $spin (br $spin))))
      (if (i32.eq (local.get $op) (i32.const 2))
        (then (return (memory.grow (local.get $n)))))
      (if (i32.eq (local.get $op) (i32.const 3))
        (then (return (table.grow (ref.null func) (local.get $n)))))
      (if (i32.eq (local.get $op) (i32.const 4))
        (then (return (call $recurse (i32.const 0)))))
      (if (i32.eq (local.get $op) (i32.const 5)) (then (unreachable)))
      (if (i32.eq (local.get $op) (i32.const 6))
        (then
          (loop $ticks
            (if (i32.lt_u (local.get $i) (local.get $n))
              (then
                (call $tick)
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $ticks))))
          (return (local.get $i))))
      (if (i32.eq (local.get $op) (i32.const 7))
        (then
          (global.set $count (i32.add (global.get $count) (i32.const 1)))
          (return (global.get $count))))
      (i32.const -1)))
  (core instance $host (export "tick" (func $tick_lowered)))
  (core instance $i (instantiate $m (with "host" (instance $host))))
  (func $run (param "op" u32) (param "n" u32) (result u32)
    (canon lift (core func $i "run")))
  (export "run" (func $run)))
"#;

struct FixtureWorld;

impl fixture::FixtureImports for HostCtx<()> {
    fn tick(&mut self) -> wasmtime::Result<()> {
        self.budget_mut().charge(0)
    }
}

impl World for FixtureWorld {
    type Data = ();
    type Pre = fixture::FixturePre<HostCtx<()>>;
    type Bindings = fixture::Fixture;

    fn link(linker: &mut Linker<HostCtx<()>>) -> wasmtime::Result<()> {
        fixture::Fixture::add_to_linker::<_, HasSelf<HostCtx<()>>>(linker, |ctx| ctx)
    }

    fn pre(pre: InstancePre<HostCtx<()>>) -> wasmtime::Result<Self::Pre> {
        fixture::FixturePre::new(pre)
    }

    fn instantiate(
        pre: &Self::Pre,
        store: &mut Store<HostCtx<()>>,
    ) -> wasmtime::Result<Self::Bindings> {
        pre.instantiate(store)
    }
}

fn guest() -> Vec<u8> {
    wat::parse_str(GUEST).unwrap()
}

/// Caps small enough that each limit trips in a few milliseconds.
fn test_config() -> WasmConfig {
    WasmConfig {
        max_instances: 4,
        max_memory_bytes: 2 * 1024 * 1024,
        max_table_elements: 100,
        call_timeout_ms: 20,
        max_host_calls: 10,
        ..WasmConfig::default()
    }
}

const UNSIGNED_OK: TrustPolicy = TrustPolicy {
    allow_unsigned: true,
    sha256: None,
};

fn load(config: WasmConfig) -> Arc<Plugin<FixtureWorld>> {
    let engine = WasmEngine::new(config).unwrap();
    let bytes = guest();
    engine
        .load::<FixtureWorld>(
            PluginSource::Bytes {
                name: "fixture",
                bytes: &bytes,
            },
            &UNSIGNED_OK,
        )
        .unwrap()
}

fn run(instance: &mut Instance<FixtureWorld>, op: u32, n: u32) -> Result<u32, CallError> {
    instance.call(|bindings, store| bindings.call_run(store, op, n))
}

#[test]
fn each_limit_traps_with_its_outcome_and_the_next_call_recovers() {
    let plugin = load(test_config());
    let mut instance = plugin.instance(()).unwrap();
    let cases: [(&str, u32, u32, Result<u32, &str>); 9] = [
        ("a plain call", 0, 7, Ok(7)),
        ("a spin past the deadline", 1, 0, Err("timeout")),
        ("memory within the cap", 2, 1, Ok(1)),
        ("memory past the cap", 2, 100, Err("memory")),
        ("a table past the cap", 3, 1_000, Err("table")),
        ("unbounded recursion", 4, 0, Err("trap")),
        ("an unreachable", 5, 0, Err("trap")),
        ("imports within the budget", 6, 10, Ok(10)),
        ("imports past the budget", 6, 11, Err("budget")),
    ];
    for (case, op, n, want) in cases {
        let got = run(&mut instance, op, n).map_err(|e| e.outcome());
        assert_eq!(got, want, "{case}");
        assert_eq!(
            run(&mut instance, 0, 1).ok(),
            Some(1),
            "{case}: the next call recovers"
        );
    }
}

#[test]
fn a_stack_overflow_reports_its_trap_code() {
    let plugin = load(test_config());
    let mut instance = plugin.instance(()).unwrap();
    assert!(matches!(
        run(&mut instance, 4, 0),
        Err(CallError::Trap(Trap::StackOverflow))
    ));
}

#[test]
fn a_trap_or_the_recycle_count_replaces_the_guest_and_keeps_the_instance() {
    let config = WasmConfig {
        recycle_after_calls: 3,
        ..test_config()
    };
    let plugin = load(config);
    let mut instance = plugin.instance(()).unwrap();
    let counts: Vec<u32> = (0..4).map(|_| run(&mut instance, 7, 0).unwrap()).collect();
    assert_eq!(
        counts,
        [1, 2, 3, 1],
        "the fourth call runs in a fresh guest"
    );

    assert!(run(&mut instance, 5, 0).is_err());
    assert_eq!(
        run(&mut instance, 7, 0).unwrap(),
        1,
        "a trap starts a fresh guest"
    );
}

#[test]
fn traps_in_a_row_mark_the_plugin_unhealthy_and_a_success_resets_the_count() {
    let config = WasmConfig {
        unhealthy_after_traps: 3,
        ..test_config()
    };
    let plugin = load(config);
    let mut instance = plugin.instance(()).unwrap();

    for _ in 0..2 {
        assert!(run(&mut instance, 5, 0).is_err());
    }
    assert_eq!(
        run(&mut instance, 0, 1).unwrap(),
        1,
        "two traps leave it healthy"
    );
    for _ in 0..3 {
        assert!(run(&mut instance, 5, 0).is_err());
    }
    assert!(!plugin.is_healthy());
    assert!(matches!(
        run(&mut instance, 0, 1),
        Err(CallError::Unhealthy(3))
    ));
    assert!(matches!(plugin.instance(()), Err(CallError::Unhealthy(3))));
}

#[test]
fn a_full_pool_refuses_an_instance_as_capacity() {
    let config = WasmConfig {
        max_instances: 1,
        ..test_config()
    };
    let plugin = load(config);
    let first = plugin.instance(()).unwrap();
    assert!(matches!(plugin.instance(()), Err(CallError::Capacity)));
    drop(first);
    assert!(
        plugin.instance(()).is_ok(),
        "a dropped instance frees its slot"
    );
}

/// A case name, the bytes, the policy, the engine's caps, and the outcome.
type LoadCase<'a> = (
    &'a str,
    &'a [u8],
    &'a TrustPolicy,
    Option<WasmConfig>,
    Result<(), &'a str>,
);

#[test]
fn the_loader_refuses_each_unsafe_plugin_before_any_guest_code_runs() {
    let good = guest();
    let digest: [u8; 32] = sha2::Sha256::digest(&good).into();
    let precompiled = wasmtime::Engine::default()
        .precompile_component(&good)
        .unwrap();
    let sockets =
        wat::parse_str(r#"(component (import "wasi:sockets/tcp@0.2.6" (instance)))"#).unwrap();
    let mut flipped = good.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 0xff;
    let pinned = TrustPolicy {
        allow_unsigned: true,
        sha256: Some(digest),
    };
    let small = WasmConfig {
        max_module_bytes: 16,
        ..test_config()
    };

    let cases: [LoadCase<'_>; 9] = [
        (
            "unsigned, refused by default",
            &good,
            &TrustPolicy::default(),
            None,
            Err("unsigned"),
        ),
        (
            "not WebAssembly",
            b"not wasm at all",
            &UNSIGNED_OK,
            None,
            Err("not_wasm"),
        ),
        (
            "precompiled code",
            &precompiled,
            &UNSIGNED_OK,
            None,
            Err("precompiled"),
        ),
        (
            "over the size cap",
            &good,
            &UNSIGNED_OK,
            Some(small),
            Err("too_large"),
        ),
        (
            "a flipped byte against the pin",
            &flipped,
            &pinned,
            None,
            Err("digest_mismatch"),
        ),
        (
            "a sockets import",
            &sockets,
            &UNSIGNED_OK,
            None,
            Err("forbidden_import"),
        ),
        (
            "a truncated file",
            &good[..good.len() / 2],
            &UNSIGNED_OK,
            None,
            Err("compile_error"),
        ),
        ("the pinned digest", &good, &pinned, None, Ok(())),
        ("unsigned, allowed", &good, &UNSIGNED_OK, None, Ok(())),
    ];
    for (case, bytes, trust, config, want) in cases {
        let engine = WasmEngine::new(config.unwrap_or_else(test_config)).unwrap();
        let got = engine
            .load::<FixtureWorld>(
                PluginSource::Bytes {
                    name: "case",
                    bytes,
                },
                trust,
            )
            .map(|_| ())
            .map_err(|e| e.outcome());
        assert_eq!(got, want, "{case}");
    }
}

#[test]
fn a_plugin_loads_from_a_path_named_after_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("enrich.wasm");
    std::fs::write(&path, guest()).unwrap();
    let engine = WasmEngine::new(test_config()).unwrap();
    let plugin = engine
        .load::<FixtureWorld>(PluginSource::Path(&path), &UNSIGNED_OK)
        .unwrap();
    assert_eq!(plugin.name(), "enrich");

    let missing = dir.path().join("missing.wasm");
    let err = engine
        .load::<FixtureWorld>(PluginSource::Path(&missing), &UNSIGNED_OK)
        .err()
        .map(|e| e.outcome());
    assert_eq!(err, Some("read_error"));
}
