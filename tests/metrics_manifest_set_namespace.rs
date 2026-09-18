// Project:   scalo
// File:      tests/metrics_manifest_set_namespace.rs
// Purpose:   The generated manifest names what a namespaced service serves
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A service that sets `metrics.namespace: acme` serves `acme_`-prefixed names,
//! and the manifest `generate-artefacts` writes for it lists those same names,
//! read from the same config the service loads.
//!
//! Config installs once per process, so the bare case is its own file.

#![cfg(feature = "cli-service")]

#[path = "common/manifest_probe.rs"]
mod manifest_probe;

use manifest_probe::{APP_NAME, manifests_for, names, repeated};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_namespaced_service_manifest_lists_the_names_it_serves() {
    let manifests = manifests_for(
        "metrics:\n  \
           namespace: acme\n  \
           otel:\n    \
             enabled: false\n\
         otel_tracing:\n  \
           enabled: false\n",
    )
    .await;

    let generated = names(&manifests.generated);
    assert!(
        generated.iter().any(|n| n == "acme_transport_sent_total"),
        "the configured namespace, not the app name: {generated:?}"
    );
    assert!(
        generated.iter().all(|n| n.starts_with("acme_")),
        "every name carries the one prefix: {generated:?}"
    );
    assert_eq!(
        generated,
        names(&manifests.served),
        "the manifest names exactly what the running service registers"
    );
    assert!(
        repeated(&manifests.served).is_empty(),
        "each served name once: {:?}",
        repeated(&manifests.served)
    );
    assert_eq!(manifests.generated.app, APP_NAME);
    let raw: serde_json::Value = serde_json::from_str(&manifests.written).unwrap();
    assert_eq!(
        raw["namespace"], "acme",
        "the namespace is carried apart from the app"
    );
}
