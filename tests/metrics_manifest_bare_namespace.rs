// Project:   scalo
// File:      tests/metrics_manifest_bare_namespace.rs
// Purpose:   The generated manifest names what a service with no namespace serves
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A service that sets no `metrics.namespace` serves bare names, and the
//! manifest `generate-artefacts` writes for it lists those same names: the
//! scalo runtime set, once each, without the app name in front.
//!
//! Config installs once per process, so the namespaced case is its own file.

#![cfg(feature = "cli-service")]

#[path = "common/manifest_probe.rs"]
mod manifest_probe;

use manifest_probe::{APP_NAME, manifests_for, names, repeated};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_service_manifest_lists_the_names_it_serves() {
    let manifests = manifests_for(
        "metrics:\n  \
           otel:\n    \
             enabled: false\n\
         otel_tracing:\n  \
           enabled: false\n",
    )
    .await;

    let generated = names(&manifests.generated);
    assert!(
        generated.iter().any(|n| n == "transport_sent_total"),
        "the scalo runtime set is in a manifest the app added nothing to: {generated:?}"
    );
    assert!(
        !generated.iter().any(|n| n.starts_with(APP_NAME)),
        "no app-name prefix the service never serves: {generated:?}"
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
    assert_eq!(manifests.served.app, APP_NAME);
    assert!(
        manifests.written.ends_with("}\n"),
        "one trailing newline, as every other artefact"
    );
}
