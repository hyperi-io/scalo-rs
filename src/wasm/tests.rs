// Project:   scalo
// File:      src/wasm/tests.rs
// Purpose:   The wasm host's limits, loader and recycling against a fixture guest
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;

use sha2::Digest;
use sigstore_verify::bundle::builder::{BundleV03, VerificationMaterialV03};
use sigstore_verify::crypto::KeyPair;
use sigstore_verify::types::bundle::{MessageDigest, MessageSignature, SignatureContent};
use sigstore_verify::types::{DigestBytes, HashAlgorithm};
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
    signers: Vec::new(),
};

fn load(config: WasmConfig) -> Arc<Plugin<FixtureWorld>> {
    let engine = WasmEngine::new(config).unwrap();
    let bytes = guest();
    engine
        .load::<FixtureWorld>(unsigned("fixture", &bytes), &UNSIGNED_OK)
        .unwrap()
}

fn unsigned<'a>(name: &'a str, bytes: &'a [u8]) -> PluginSource<'a> {
    PluginSource::Bytes {
        name,
        bytes,
        bundle: None,
        signature: None,
    }
}

/// A fresh P-256 key and the [`Signer`] that trusts it.
fn operator_key() -> (KeyPair, Signer) {
    let pair = KeyPair::generate_ecdsa_p256().unwrap();
    let public_key = pair.public_key_der().unwrap().to_pem();
    (pair, Signer::Key { public_key })
}

/// The base64 signature over `bytes` that `cosign sign-blob --key` writes.
fn sign(pair: &KeyPair, bytes: &[u8]) -> Vec<u8> {
    format!("{}\n", pair.sign(bytes).unwrap().to_base64()).into_bytes()
}

/// A bundle that parses but signs nothing these tests trust: a key-hint
/// bundle with no certificate, so no keyless signer can accept it.
fn keyless_impostor(pair: &KeyPair, bytes: &[u8]) -> Vec<u8> {
    let digest: [u8; 32] = sha2::Sha256::digest(bytes).into();
    let signature = MessageSignature::new(pair.sign(bytes).unwrap()).with_message_digest(
        MessageDigest::new(HashAlgorithm::Sha2256, DigestBytes::new(digest.to_vec())),
    );
    BundleV03::new(
        VerificationMaterialV03::PublicKey {
            hint: "operator".to_string(),
        },
        SignatureContent::MessageSignature(signature),
    )
    .into_bundle()
    .to_json()
    .unwrap()
    .into_bytes()
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
        sha256: Some(super::engine::hex(&digest)),
        ..UNSIGNED_OK
    };
    let bad_pin = TrustPolicy {
        sha256: Some("zz".to_string()),
        ..UNSIGNED_OK
    };
    let small = WasmConfig {
        max_module_bytes: 16,
        ..test_config()
    };

    let cases: [LoadCase<'_>; 10] = [
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
        (
            "a pin that isn't hex",
            &good,
            &bad_pin,
            None,
            Err("bad_policy"),
        ),
        ("the pinned digest", &good, &pinned, None, Ok(())),
        ("unsigned, allowed", &good, &UNSIGNED_OK, None, Ok(())),
    ];
    for (case, bytes, trust, config, want) in cases {
        let engine = WasmEngine::new(config.unwrap_or_else(test_config)).unwrap();
        let got = engine
            .load::<FixtureWorld>(unsigned("case", bytes), trust)
            .map(|_| ())
            .map_err(|e| e.outcome());
        assert_eq!(got, want, "{case}");
    }
}

#[test]
fn a_signature_loads_only_when_a_trusted_signer_made_it_over_these_bytes() {
    let good = guest();
    let mut changed = good.clone();
    let last = changed.len() - 1;
    changed[last] ^= 0xff;
    let (trusted, signer) = operator_key();
    let (stranger, _) = operator_key();
    let trusted_signature = sign(&trusted, &good);
    let stranger_signature = sign(&stranger, &good);
    let trusts_key = TrustPolicy {
        signers: vec![signer.clone()],
        ..TrustPolicy::default()
    };
    let keyless_and_key = TrustPolicy {
        signers: vec![
            Signer::Keyless {
                identity:
                    "https://github.com/acme/plugins/.github/workflows/release.yml@refs/heads/main"
                        .to_string(),
                issuer: "https://token.actions.githubusercontent.com".to_string(),
            },
            signer.clone(),
        ],
        ..TrustPolicy::default()
    };
    let trusts_nobody = TrustPolicy::default();
    let impostor = keyless_impostor(&trusted, &good);

    // (case, bytes, bundle, signature, policy, outcome)
    let cases: [SignCase<'_>; 12] = [
        (
            "a trusted key",
            &good,
            None,
            Some(&trusted_signature),
            &trusts_key,
            Ok(()),
        ),
        (
            "a bundle no keyless signer can judge, beside a trusted signature",
            &good,
            Some(&impostor),
            Some(&trusted_signature),
            &trusts_key,
            Ok(()),
        ),
        (
            "a bad bundle beside a trusted signature",
            &good,
            Some(&impostor),
            Some(&trusted_signature),
            &keyless_and_key,
            Err("bad_signature"),
        ),
        (
            "a trusted key beside a keyless signer",
            &good,
            None,
            Some(&trusted_signature),
            &keyless_and_key,
            Ok(()),
        ),
        (
            "another key",
            &good,
            None,
            Some(&stranger_signature),
            &trusts_key,
            Err("untrusted_signer"),
        ),
        (
            "bytes changed after signing",
            &changed,
            None,
            Some(&trusted_signature),
            &trusts_key,
            Err("untrusted_signer"),
        ),
        (
            "a signature that isn't base64",
            &good,
            None,
            Some(b"%%%"),
            &trusts_key,
            Err("bad_signature"),
        ),
        (
            "a bundle that isn't one",
            &good,
            Some(b"{}"),
            None,
            &keyless_and_key,
            Err("bad_signature"),
        ),
        (
            "a bundle with no certificate",
            &good,
            Some(&impostor),
            None,
            &keyless_and_key,
            Err("bad_signature"),
        ),
        (
            "a bundle and a policy with no keyless signer",
            &good,
            Some(&impostor),
            None,
            &trusts_key,
            Err("untrusted_signer"),
        ),
        (
            "a policy naming no signer",
            &good,
            None,
            Some(&trusted_signature),
            &trusts_nobody,
            Err("untrusted_signer"),
        ),
        (
            "a bad signature where unsigned is allowed",
            &good,
            None,
            Some(b"%%%"),
            &UNSIGNED_OK,
            Err("bad_signature"),
        ),
    ];
    let engine = WasmEngine::new(test_config()).unwrap();
    for (case, bytes, bundle, signature, trust, want) in cases {
        let source = PluginSource::Bytes {
            name: "case",
            bytes,
            bundle,
            signature,
        };
        let got = engine
            .load::<FixtureWorld>(source, trust)
            .map(|_| ())
            .map_err(|e| e.outcome());
        assert_eq!(got, want, "{case}");
    }

    let source = PluginSource::Bytes {
        name: "case",
        bytes: &good,
        bundle: None,
        signature: Some(&trusted_signature),
    };
    let plugin = engine.load::<FixtureWorld>(source, &trusts_key).unwrap();
    assert_eq!(plugin.signer(), signer.label());
    assert!(plugin.signer().starts_with("key:"));
}

/// A case name, the bytes, the bundle, the signature, the policy and the
/// outcome.
type SignCase<'a> = (
    &'a str,
    &'a [u8],
    Option<&'a [u8]>,
    Option<&'a [u8]>,
    &'a TrustPolicy,
    Result<(), &'a str>,
);

#[test]
fn a_signed_plugin_loads_from_a_path_with_its_signature_beside_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("enrich.wasm");
    let bytes = guest();
    let (pair, signer) = operator_key();
    std::fs::write(&path, &bytes).unwrap();
    std::fs::write(dir.path().join("enrich.wasm.sig"), sign(&pair, &bytes)).unwrap();
    let policy = TrustPolicy {
        signers: vec![signer],
        ..TrustPolicy::default()
    };
    let engine = WasmEngine::new(test_config()).unwrap();
    let plugin = engine
        .load::<FixtureWorld>(PluginSource::Path(&path), &policy)
        .unwrap();
    assert!(plugin.signer().starts_with("key:"));
}

#[test]
fn one_wasm_compiles_once_while_a_plugin_holds_it() {
    let engine = WasmEngine::new(test_config()).unwrap();
    let bytes = guest();
    let first = engine
        .load::<FixtureWorld>(unsigned("a", &bytes), &UNSIGNED_OK)
        .unwrap();
    let second = engine
        .load::<FixtureWorld>(unsigned("b", &bytes), &UNSIGNED_OK)
        .unwrap();
    assert_eq!(engine.cached_components(), 1);
    drop(first);
    assert_eq!(engine.cached_components(), 1, "the second plugin holds it");
    drop(second);
    assert_eq!(engine.cached_components(), 0);

    let sockets =
        wat::parse_str(r#"(component (import "wasi:sockets/tcp@0.2.6" (instance)))"#).unwrap();
    assert!(
        engine
            .load::<FixtureWorld>(unsigned("sockets", &sockets), &UNSIGNED_OK)
            .is_err()
    );
    assert_eq!(
        engine.cached_components(),
        0,
        "a refused load holds nothing"
    );
}

#[test]
fn the_info_gauge_has_one_owner_per_live_plugin_with_its_labels() {
    let engine = WasmEngine::new(test_config()).unwrap();
    let bytes = guest();
    let digest: [u8; 32] = sha2::Sha256::digest(&bytes).into();
    let labels = [
        "fixture".to_string(),
        super::engine::hex(&digest),
        "unsigned".to_string(),
    ];
    let old = engine
        .load::<FixtureWorld>(unsigned("fixture", &bytes), &UNSIGNED_OK)
        .unwrap();
    let new = engine
        .load::<FixtureWorld>(unsigned("fixture", &bytes), &UNSIGNED_OK)
        .unwrap();
    assert_eq!(engine.info_owners(&labels), 2);
    drop(old);
    assert_eq!(
        engine.info_owners(&labels),
        1,
        "the same-digest reload is still live"
    );
    drop(new);
    assert_eq!(engine.info_owners(&labels), 0);
}

/// A guest whose start function traps, so every instantiation fails.
const TRAPS_AT_START: &str = r#"
(component
  (import "tick" (func $tick))
  (core module $m
    (func $boom (unreachable))
    (start $boom)
    (func (export "run") (param i32 i32) (result i32) (i32.const 0)))
  (core instance $i (instantiate $m))
  (func $run (param "op" u32) (param "n" u32) (result u32)
    (canon lift (core func $i "run")))
  (export "run" (func $run)))
"#;

#[test]
fn a_guest_that_traps_at_start_up_counts_toward_unhealthy() {
    let config = WasmConfig {
        unhealthy_after_traps: 3,
        ..test_config()
    };
    let engine = WasmEngine::new(config).unwrap();
    let bytes = wat::parse_str(TRAPS_AT_START).unwrap();
    let plugin = engine
        .load::<FixtureWorld>(unsigned("boom", &bytes), &UNSIGNED_OK)
        .unwrap();
    for _ in 0..3 {
        assert!(matches!(plugin.instance(()), Err(CallError::Trap(_))));
    }
    assert!(!plugin.is_healthy());
    assert!(matches!(plugin.instance(()), Err(CallError::Unhealthy(3))));
}

#[test]
fn a_replaced_plugin_takes_over_each_instance_at_its_next_call() {
    let engine = WasmEngine::new(test_config()).unwrap();
    let bytes = guest();
    let load = || {
        engine
            .load::<FixtureWorld>(unsigned("fixture", &bytes), &UNSIGNED_OK)
            .unwrap()
    };
    let slot = PluginSlot::new(load());
    let mut instance = slot.instance(()).unwrap();
    assert_eq!(run(&mut instance, 7, 0).unwrap(), 1);
    assert_eq!(run(&mut instance, 7, 0).unwrap(), 2);

    let next = load();
    let old = slot.replace(Arc::clone(&next));
    assert_eq!(
        run(&mut instance, 7, 0).unwrap(),
        1,
        "the first call after the swap succeeds, in a fresh guest of the new plugin"
    );
    assert!(Arc::ptr_eq(instance.plugin(), &next));
    drop(old);
    assert_eq!(run(&mut instance, 7, 0).unwrap(), 2);
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

/// A pull from a real registry, through the shared test containers.
#[cfg(all(feature = "wasm-oci", feature = "deployment-test-support"))]
mod registry {
    use std::time::Duration;

    use oci_client::client::{ClientConfig, ClientProtocol, Config, ImageLayer};
    use oci_client::secrets::RegistryAuth;
    use oci_client::{Client, Reference};

    use super::{FixtureWorld, guest, operator_key, run, sign};
    use crate::deployment::test_support::shared_service::{self, Ready, ServiceSpec, Suite};
    use crate::wasm::oci::{
        OciAuth, PullError, SIGNATURE_MEDIA_TYPE, WASM_MEDIA_TYPE, pull_insecure_http,
    };
    use crate::wasm::{PluginSource, TrustPolicy, WasmConfig, WasmEngine};

    /// renovate: datasource=docker depName=registry
    const REGISTRY_IMAGE: &str =
        "registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373";

    const REGISTRY_PORT: u16 = 5000;

    const SUITE: Suite = Suite {
        repo: "scalo-rs",
        manifest_dir: env!("CARGO_MANIFEST_DIR"),
        tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
    };

    fn registry_spec() -> ServiceSpec {
        ServiceSpec {
            ports: vec![REGISTRY_PORT],
            ready: Ready::Http {
                port: REGISTRY_PORT,
                path: "/v2/",
                body: "{}",
            },
            ..ServiceSpec::new("registry", REGISTRY_IMAGE, Duration::from_secs(60))
        }
    }

    /// Pushes the `.wasm` and its key signature as one artefact.
    async fn push(reference: &str, wasm: &[u8], signature: &[u8]) {
        let client = Client::new(ClientConfig {
            protocol: ClientProtocol::Http,
            ..ClientConfig::default()
        });
        let reference: Reference = reference.parse().expect("a reference");
        let layers = [
            ImageLayer::new(wasm.to_vec(), WASM_MEDIA_TYPE.to_string(), None),
            ImageLayer::new(signature.to_vec(), SIGNATURE_MEDIA_TYPE.to_string(), None),
        ];
        let config = Config::new(
            b"{}".to_vec(),
            "application/vnd.oci.empty.v1+json".to_string(),
            None,
        );
        client
            .push(&reference, &layers, config, &RegistryAuth::Anonymous, None)
            .await
            .expect("push the plugin");
    }

    #[tokio::test]
    #[ignore = "needs Docker for a registry:2 container -- run: cargo nextest run --run-ignored all --ignore-default-filter"]
    async fn a_plugin_pulled_from_a_registry_loads_only_under_its_signer() {
        if !crate::deployment::test_support::docker_available() {
            assert!(
                std::env::var_os("CI").is_none(),
                "no Docker daemon answers in CI, which provides a container runtime"
            );
            eprintln!("skipping: no Docker daemon answers");
            return;
        }
        let container =
            tokio::task::spawn_blocking(|| shared_service::acquire(&SUITE, &registry_spec()))
                .await
                .expect("start the registry");
        let reference = format!("{}/plugins/fixture:1.0.0", container.addr(REGISTRY_PORT));

        let bytes = guest();
        let (pair, signer) = operator_key();
        let signature = sign(&pair, &bytes);
        push(&reference, &bytes, &signature).await;

        let pulled = pull_insecure_http(&reference, OciAuth::Anonymous, 1024 * 1024)
            .await
            .expect("pull the plugin");
        assert_eq!(pulled.wasm, bytes);
        assert!(pulled.bundle.is_none());
        assert_eq!(pulled.signature.as_deref(), Some(signature.as_slice()));

        let engine = WasmEngine::new(WasmConfig::default()).expect("an engine");
        let trusted = TrustPolicy {
            allow_unsigned: false,
            sha256: None,
            signers: vec![signer],
        };
        let plugin = engine
            .load::<FixtureWorld>(
                PluginSource::Bytes {
                    name: "fixture",
                    bytes: &pulled.wasm,
                    bundle: pulled.bundle.as_deref(),
                    signature: pulled.signature.as_deref(),
                },
                &trusted,
            )
            .expect("the pulled plugin loads under its signer");
        let mut instance = plugin.instance(()).expect("an instance");
        assert_eq!(run(&mut instance, 0, 7).expect("a call"), 7);

        let (_, stranger) = operator_key();
        let refused = engine.load::<FixtureWorld>(
            PluginSource::Bytes {
                name: "fixture",
                bytes: &pulled.wasm,
                bundle: None,
                signature: pulled.signature.as_deref(),
            },
            &TrustPolicy {
                allow_unsigned: false,
                sha256: None,
                signers: vec![stranger],
            },
        );
        assert_eq!(refused.err().map(|e| e.outcome()), Some("untrusted_signer"));

        let capped = pull_insecure_http(&reference, OciAuth::Anonymous, 16).await;
        assert!(
            matches!(capped, Err(PullError::TooLarge { .. })),
            "a layer over the cap is refused"
        );
    }
}
