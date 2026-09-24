// Project:   scalo
// File:      tests/pipe_filter_tiers.rs
// Purpose:   Pipe transport compiles its filters against the cascade tier gates
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The pipe transport reads `transport.filter_tiers` from the config cascade,
//! the same gates every other backend compiles its filter rules against.
//!
//! The global config installs once per process, so this file owns it.

#![cfg(all(feature = "transport-pipe", feature = "config", feature = "expression"))]

use scalo::transport::filter::{FilterAction, FilterRule};
use scalo::transport::{PipeTransport, PipeTransportConfig, TransportBase, TransportSender};

#[tokio::test]
async fn pipe_compiles_filters_against_the_cascade_tier_gates() {
    let dir = tempfile::tempdir().expect("config tempdir");
    std::fs::write(
        dir.path().join("settings.yaml"),
        "transport:\n  filter_tiers:\n    allow_cel_filters_out: true\n",
    )
    .expect("write settings.yaml");

    scalo::config::setup(scalo::config::ConfigOptions {
        config_paths: vec![dir.path().to_path_buf()],
        load_dotenv: false,
        ..scalo::config::ConfigOptions::default()
    })
    .expect("config setup");

    // Tier 2: the default gates reject it, so it compiles only when the pipe
    // reads the gate the cascade opened.
    let transport = PipeTransport::new(&PipeTransportConfig {
        filters_out: vec![FilterRule {
            expression: "severity > 3".into(),
            action: FilterAction::Dlq,
        }],
        ..PipeTransportConfig::default()
    });

    assert!(
        transport.is_healthy(),
        "the Tier 2 rule must compile once the cascade opens its gate"
    );

    let result = transport
        .send("ignored", bytes::Bytes::from_static(br#"{"severity":5}"#))
        .await;
    assert!(
        result.is_filtered_dlq(),
        "a record matching the dlq rule must come back for DLQ routing, got {result:?}"
    );
}
