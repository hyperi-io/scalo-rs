// Project:   scalo
// File:      src/concurrency/periodic.rs
// Purpose:   PeriodicWorker -- timer-driven loop with biased shutdown
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Timer-driven background loop, generic over the task type.
//!
//! Use for everything that's "do X every N seconds": config reload
//! polling, scaling pressure recompute, periodic health snapshots,
//! version-check pings to crates.io. Encapsulates the
//! `tokio::time::interval` + `tokio::select! { biased; shutdown | tick }`
//! boilerplate that would otherwise be open-coded per call site.
//!
//! # Shape
//!
//! ```text
//! tokio::time::interval --tick--> actor task --tick()--> PeriodicTask impl
//!                                     ^
//!                                     | biased select
//!                                     |
//!                              CancellationToken
//! ```
//!
//! Tick errors are logged at WARN and do NOT terminate the worker --
//! the next tick still fires. Use the `shutdown` hook for cleanup.

use std::future::Future;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::error::TickError;

/// A task that runs on a fixed interval.
///
/// Implementations may hold mutable state -- `tick` takes `&mut self`.
/// Replies / outputs propagate through the state itself (`AtomicU64`
/// counters, `ArcSwap<T>` config handles, etc.) -- `tick` returns
/// `Result<(), TickError>` purely for error signalling.
pub trait PeriodicTask: Send + 'static {
    /// Called once per interval tick.
    fn tick(&mut self) -> impl Future<Output = Result<(), TickError>> + Send;

    /// Called once after the shutdown token fires, before the worker
    /// task exits. Default: no-op.
    fn shutdown(&mut self) -> impl Future<Output = Result<(), TickError>> + Send {
        std::future::ready(Ok(()))
    }
}

/// Handle for the worker task.
///
/// Dropping the handle does NOT abort the task -- the task lives until
/// the `CancellationToken` is fired or the runtime shuts down. Use
/// [`Self::join`] for graceful drain after signalling shutdown.
pub struct PeriodicWorker {
    join: JoinHandle<()>,
}

impl PeriodicWorker {
    /// Spawn a periodic worker. The first `tick()` fires after one
    /// `interval_duration` elapses (NOT at t=0). This avoids the
    /// common bug where every spawned worker bursts a tick immediately
    /// on startup, hammering downstream dependencies during deploy.
    pub fn spawn<T: PeriodicTask>(
        task: T,
        interval_duration: Duration,
        shutdown: CancellationToken,
    ) -> Self {
        Self::start(task, interval_duration, interval_duration, shutdown)
    }

    /// [`spawn`](Self::spawn), with the first `tick()` at once: for a task
    /// whose output is read from startup, such as a gauge or a pool size.
    pub fn spawn_immediate<T: PeriodicTask>(
        task: T,
        interval_duration: Duration,
        shutdown: CancellationToken,
    ) -> Self {
        Self::start(task, Duration::ZERO, interval_duration, shutdown)
    }

    fn start<T: PeriodicTask>(
        mut task: T,
        first_tick: Duration,
        interval_duration: Duration,
        shutdown: CancellationToken,
    ) -> Self {
        let join = tokio::spawn(async move {
            let mut tick = interval_at(Instant::now() + first_tick, interval_duration);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        if let Err(e) = task.shutdown().await {
                            warn!(error = %e, "periodic task shutdown hook failed");
                        }
                        return;
                    }
                    _ = tick.tick() => {
                        if let Err(e) = task.tick().await {
                            warn!(error = %e, "periodic task tick failed");
                        }
                    }
                }
            }
        });
        Self { join }
    }

    /// Await the worker's clean exit. Use after `shutdown.cancel()`.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.join.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant;

    /// Counts its ticks, and fails every one when `fail` is set.
    struct CountingTask {
        ticks: Arc<AtomicU32>,
        fail: bool,
    }

    impl PeriodicTask for CountingTask {
        async fn tick(&mut self) -> Result<(), TickError> {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(TickError::Generic("simulated".into()));
            }
            Ok(())
        }
    }

    struct ShutdownTask {
        ticks: Arc<AtomicU32>,
        shutdown_called: Arc<AtomicU32>,
    }

    impl PeriodicTask for ShutdownTask {
        async fn tick(&mut self) -> Result<(), TickError> {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), TickError> {
            self.shutdown_called.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    type Spawn = fn(CountingTask, Duration, CancellationToken) -> PeriodicWorker;

    // The clock is paused, so time advances only while every task is idle
    // and the tick count is exact whatever the host is doing.
    #[tokio::test(start_paused = true)]
    async fn ticks_by_first_tick_and_elapsed_time() {
        let delayed: Spawn = PeriodicWorker::spawn;
        let immediate: Spawn = PeriodicWorker::spawn_immediate;
        // (case, spawn, interval ms, run ms, every tick fails, ticks wanted)
        let cases = [
            ("spawn waits one interval", delayed, 100, 10, false, 0),
            ("spawn_immediate ticks at t=0", immediate, 100, 10, false, 1),
            ("110ms over 20ms", delayed, 20, 110, false, 5),
            ("110ms over 20ms, plus t=0", immediate, 20, 110, false, 6),
            ("failing ticks keep going", delayed, 15, 80, true, 5),
        ];
        for (case, spawn, interval_ms, run_ms, fail, want) in cases {
            let ticks = Arc::new(AtomicU32::new(0));
            let shutdown = CancellationToken::new();
            let worker = spawn(
                CountingTask {
                    ticks: ticks.clone(),
                    fail,
                },
                Duration::from_millis(interval_ms),
                shutdown.clone(),
            );
            tokio::time::sleep(Duration::from_millis(run_ms)).await;
            shutdown.cancel();
            worker.join().await.expect("clean exit");
            assert_eq!(ticks.load(Ordering::SeqCst), want, "{case}");
        }
    }

    #[tokio::test]
    async fn shutdown_hook_called_exactly_once() {
        let ticks = Arc::new(AtomicU32::new(0));
        let shutdown_called = Arc::new(AtomicU32::new(0));
        let shutdown = CancellationToken::new();
        let worker = PeriodicWorker::spawn(
            ShutdownTask {
                ticks: ticks.clone(),
                shutdown_called: shutdown_called.clone(),
            },
            Duration::from_mins(1), // very long, no ticks expected
            shutdown.clone(),
        );
        shutdown.cancel();
        worker.join().await.expect("clean exit");
        assert_eq!(shutdown_called.load(Ordering::SeqCst), 1);
        // No tick should have fired in the brief lifetime.
        assert_eq!(ticks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn biased_select_prioritises_shutdown_over_tick() {
        // If a tick was due simultaneously with shutdown, the biased
        // select! must pick shutdown first. We can't directly observe
        // ordering, but we can verify shutdown completes cleanly even
        // when triggered at the moment a tick would fire.
        let ticks = Arc::new(AtomicU32::new(0));
        let shutdown = CancellationToken::new();
        let worker = PeriodicWorker::spawn(
            CountingTask {
                ticks: ticks.clone(),
                fail: false,
            },
            Duration::from_millis(1), // very tight -- many tick opportunities
            shutdown.clone(),
        );
        let t0 = Instant::now();
        // Run for a bit, then cancel.
        tokio::time::sleep(Duration::from_millis(20)).await;
        shutdown.cancel();
        worker.join().await.expect("clean exit");
        let elapsed = t0.elapsed();
        // join() must return in much less than 1s -- i.e. shutdown
        // wasn't blocked by an in-flight tick.
        assert!(
            elapsed < Duration::from_millis(500),
            "worker took {elapsed:?} to shut down (expected <500ms)",
        );
    }
}
