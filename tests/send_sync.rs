// Project:   scalo
// File:      tests/send_sync.rs
// Purpose:   Compile-time Send + Sync checks for long-lived public types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A tokio app shares these types across worker threads and spawns the
//! futures they return, so each must stay `Send + Sync` and each future
//! `Send`. A change that breaks one fails this file to compile.

#[allow(
    dead_code,
    reason = "each feature-gated check calls it, and some may be off"
)]
fn assert_send_sync<T: Send + Sync>() {}

#[allow(
    dead_code,
    reason = "each feature-gated check calls it, and some may be off"
)]
fn assert_send<F: Send>(_: &F) {}

#[test]
fn compression_types_are_send_sync() {
    #[cfg(feature = "compression")]
    assert_send_sync::<scalo::compression::CompressorPool>();
}

#[cfg(any(feature = "worker-pool", feature = "worker-batch", feature = "worker"))]
#[test]
fn worker_types_are_send_sync() {
    assert_send_sync::<scalo::AdaptiveWorkerPool>();
    #[cfg(feature = "worker-batch")]
    assert_send_sync::<scalo::worker::BatchEngine>();
}

#[cfg(all(feature = "tiered-sink", feature = "transport-memory"))]
#[test]
fn tiered_sink_types_are_send_sync() {
    assert_send_sync::<scalo::TieredSink<scalo::transport::MemoryTransport>>();
    assert_send_sync::<scalo::CircuitBreaker>();
}

#[cfg(feature = "dlq")]
#[test]
fn dlq_is_send_sync() {
    assert_send_sync::<scalo::Dlq>();
}

#[cfg(feature = "metrics")]
#[test]
fn metrics_manager_is_send_sync() {
    assert_send_sync::<scalo::MetricsManager>();
}

#[cfg(feature = "config-reload")]
#[test]
fn config_types_are_send_sync() {
    assert_send_sync::<scalo::SharedConfig<String>>();
    assert_send_sync::<scalo::ConfigReloader<String>>();
}

#[cfg(feature = "health")]
#[test]
fn health_registry_is_send_sync() {
    assert_send_sync::<scalo::HealthRegistry>();
}

#[cfg(feature = "secrets")]
#[test]
fn secrets_manager_is_send_sync() {
    assert_send_sync::<scalo::SecretsManager>();
}

#[cfg(feature = "governor")]
#[test]
fn governor_types_are_send_sync() {
    assert_send_sync::<scalo::SelfRegulationGovernor>();
    assert_send_sync::<scalo::RateLimiter>();
    assert_send_sync::<scalo::InboundGate>();
    assert_send_sync::<scalo::ByteBudgetController>();
    assert_send_sync::<scalo::UnifiedPressure>();
}

#[cfg(feature = "transport")]
#[test]
fn ack_tickets_are_send_sync() {
    assert_send_sync::<scalo::transport::ack::Tickets>();
}

#[cfg(feature = "strmatch")]
#[test]
fn matchers_are_send_sync() {
    assert_send_sync::<scalo::strmatch::StrMatcher>();
    assert_send_sync::<scalo::strmatch::StrMatcherSet>();
}

#[cfg(feature = "cli-service")]
#[test]
fn service_runtime_is_send_sync() {
    assert_send_sync::<scalo::ServiceRuntime>();
}

#[cfg(all(feature = "tiered-sink", feature = "transport-memory"))]
#[test]
fn tiered_sink_send_batch_future_is_send() {
    use scalo::TieredSink;
    use scalo::transport::{MemoryTransport, Record};

    fn check(sink: &TieredSink<MemoryTransport>, records: &[Record]) {
        assert_send(&sink.send_batch(records));
    }
    let _: fn(&TieredSink<MemoryTransport>, &[Record]) = check;
}

#[cfg(all(feature = "worker-batch", feature = "transport-memory"))]
#[test]
fn batch_engine_run_futures_are_send() {
    use std::future::Ready;
    use std::time::Duration;

    use scalo::transport::{MemoryToken, MemoryTransport, WorkBatch};
    use scalo::worker::{BatchEngine, CommitMode, EngineError, ParsedBatch};
    use tokio_util::sync::CancellationToken;

    type Batch = WorkBatch<MemoryToken>;
    type Tick = fn() -> Ready<Result<(), EngineError>>;

    fn sink(_: &Batch) -> Ready<Result<(), EngineError>> {
        std::future::ready(Ok(()))
    }

    fn check(engine: &BatchEngine, receiver: &MemoryTransport) {
        let shutdown = CancellationToken::new;
        let process = |batch: Batch| Ok::<Batch, EngineError>(batch);
        let process_parsed = |parsed: ParsedBatch<'_, MemoryToken>| {
            Ok::<Batch, EngineError>(WorkBatch::new(parsed.records, parsed.commit_tokens))
        };
        assert_send(&engine.run_workbatch(
            receiver,
            shutdown(),
            process,
            sink,
            CommitMode::Auto,
            None::<(Duration, Tick)>,
        ));
        assert_send(&engine.run_workbatch_streaming(
            receiver,
            shutdown(),
            process,
            sink,
            CommitMode::Auto,
            1 << 20,
            None::<(Duration, Tick)>,
        ));
        assert_send(&engine.run_workbatch_parsed(
            receiver,
            shutdown(),
            process_parsed,
            sink,
            CommitMode::Auto,
            None::<(Duration, Tick)>,
        ));
        #[cfg(feature = "governor")]
        assert_send(&engine.run_governed(
            receiver,
            shutdown(),
            process,
            sink,
            CommitMode::Auto,
            None::<(Duration, Tick)>,
        ));
    }
    let _: fn(&BatchEngine, &MemoryTransport) = check;
}
