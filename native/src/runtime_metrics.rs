// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Tokio runtime metrics, gated behind the `runtime-metrics` Cargo feature.
//!
//! `tokio-metrics` only compiles when `--cfg tokio_unstable` is set, so the
//! whole module is conditional on the feature flag. The feature-off variant
//! still exists; `runtime_stats()` returns an `Err` whose message points at
//! the rebuild command. The Java surface (`SessionContext.runtimeStats()`)
//! is therefore stable across both build configurations.
//!
//! Field layout returned across the FFI boundary (in this exact order, all
//! `i64`):
//!   0  numWorkers
//!   1  liveTasksCount
//!   2  globalQueueDepth
//!   3  elapsedNanos
//!   4  totalBusyNanos
//!   5  totalParkCount
//!   6  totalPollsCount
//!   7  totalNoopCount
//!   8  totalStealCount
//!   9  totalLocalScheduleCount
//!   10 totalOverflowCount

#[cfg(not(feature = "runtime-metrics"))]
use datafusion_jni_common::errors::JniResult;

/// Number of i64 values in the snapshot array; kept here so the Java side and
/// the feature-off stub agree on the layout.
pub const STATS_FIELD_COUNT: usize = 11;

#[cfg(feature = "runtime-metrics")]
mod imp {
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;
    use tokio_metrics::{RuntimeIntervals, RuntimeMonitor};

    use super::STATS_FIELD_COUNT;
    use datafusion_jni_common::errors::JniResult;

    /// `RuntimeMonitor::intervals().next()` returns *delta* metrics covering
    /// the period since the previous call (or, on the very first call, since
    /// the iterator was constructed). To present the documented
    /// "totals since runtime start" semantic to Java callers, we own the
    /// iterator process-wide and accumulate every field that's documented as
    /// monotonic.
    ///
    /// Snapshot fields (`workers_count`, `live_tasks_count`,
    /// `global_queue_depth`) are point-in-time and pass through unchanged.
    struct RuntimeAccumulator {
        intervals: RuntimeIntervals,
        elapsed: Duration,
        total_busy: Duration,
        total_park_count: u64,
        total_polls_count: u64,
        total_noop_count: u64,
        total_steal_count: u64,
        total_local_schedule_count: u64,
        total_overflow_count: u64,
    }

    static ACC: OnceLock<Mutex<RuntimeAccumulator>> = OnceLock::new();

    fn accumulator() -> &'static Mutex<RuntimeAccumulator> {
        // Warm `crate::runtime()` first so the shared Tokio runtime exists
        // and its OnceLock initializer has already populated ACC via
        // `runtime_metrics::init`. We must NOT call `runtime()` from inside
        // `ACC.get_or_init`: that would acquire ACC's once-init slot, then
        // (on the very first call) recurse through `RT.get_or_init` ->
        // `init()` -> `ACC.get_or_init`, which is same-thread reentrancy on
        // the same OnceLock and deadlocks indefinitely.
        let _ = crate::runtime();
        // After `runtime()` returns, ACC is guaranteed populated. The
        // defensive `get_or_init` covers a future build path that constructs
        // the runtime without going through `crate::runtime` (none today);
        // it is safe to invoke from here because no init lock is held.
        ACC.get_or_init(build_accumulator)
    }

    fn build_accumulator() -> Mutex<RuntimeAccumulator> {
        let handle = crate::runtime().handle().clone();
        let monitor = RuntimeMonitor::new(&handle);
        Mutex::new(RuntimeAccumulator {
            intervals: monitor.intervals(),
            elapsed: Duration::ZERO,
            total_busy: Duration::ZERO,
            total_park_count: 0,
            total_polls_count: 0,
            total_noop_count: 0,
            total_steal_count: 0,
            total_local_schedule_count: 0,
            total_overflow_count: 0,
        })
    }

    /// Eagerly construct the runtime-metrics accumulator. Called from
    /// `crate::runtime()` immediately after the shared Tokio runtime is
    /// created so the accumulator's interval baseline matches runtime start.
    /// Without this, `RuntimeMonitor` would only start sampling at the first
    /// `runtimeStats()` call, silently dropping every poll/busy nanosecond
    /// that happened before that.
    pub fn init(handle: &tokio::runtime::Handle) {
        ACC.get_or_init(|| {
            let monitor = RuntimeMonitor::new(handle);
            Mutex::new(RuntimeAccumulator {
                intervals: monitor.intervals(),
                elapsed: Duration::ZERO,
                total_busy: Duration::ZERO,
                total_park_count: 0,
                total_polls_count: 0,
                total_noop_count: 0,
                total_steal_count: 0,
                total_local_schedule_count: 0,
                total_overflow_count: 0,
            })
        });
    }

    pub fn runtime_stats() -> JniResult<[i64; STATS_FIELD_COUNT]> {
        let mut acc = accumulator()
            .lock()
            .map_err(|_| "runtime-metrics accumulator lock poisoned")?;
        let delta = acc
            .intervals
            .next()
            .ok_or("tokio-metrics RuntimeMonitor returned no interval")?;
        // Workers count and the point-in-time fields are not deltas; they are
        // the runtime's current state. Pass through directly.
        let workers_count = delta.workers_count;
        let live_tasks_count = delta.live_tasks_count;
        let global_queue_depth = delta.global_queue_depth;
        // Accumulate the deltas.
        acc.elapsed = acc.elapsed.saturating_add(delta.elapsed);
        acc.total_busy = acc.total_busy.saturating_add(delta.total_busy_duration);
        acc.total_park_count = acc.total_park_count.saturating_add(delta.total_park_count);
        acc.total_polls_count = acc
            .total_polls_count
            .saturating_add(delta.total_polls_count);
        acc.total_noop_count = acc.total_noop_count.saturating_add(delta.total_noop_count);
        acc.total_steal_count = acc
            .total_steal_count
            .saturating_add(delta.total_steal_count);
        acc.total_local_schedule_count = acc
            .total_local_schedule_count
            .saturating_add(delta.total_local_schedule_count);
        acc.total_overflow_count = acc
            .total_overflow_count
            .saturating_add(delta.total_overflow_count);

        Ok([
            workers_count as i64,
            live_tasks_count as i64,
            global_queue_depth as i64,
            i128_to_i64_saturating(acc.elapsed.as_nanos() as i128),
            i128_to_i64_saturating(acc.total_busy.as_nanos() as i128),
            acc.total_park_count as i64,
            acc.total_polls_count as i64,
            acc.total_noop_count as i64,
            acc.total_steal_count as i64,
            acc.total_local_schedule_count as i64,
            acc.total_overflow_count as i64,
        ])
    }

    fn i128_to_i64_saturating(v: i128) -> i64 {
        v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }
}

#[cfg(feature = "runtime-metrics")]
pub use imp::{init, runtime_stats};

/// No-op when the `runtime-metrics` feature is off: there is no monitor to
/// initialise. Kept as a regular function (rather than a `cfg` annotation at
/// every call site) so the runtime construction path stays unconditional.
#[cfg(not(feature = "runtime-metrics"))]
pub fn init(_handle: &tokio::runtime::Handle) {}

#[cfg(not(feature = "runtime-metrics"))]
pub fn runtime_stats() -> JniResult<[i64; STATS_FIELD_COUNT]> {
    Err(
        "datafusion-jni was built without the `runtime-metrics` Cargo feature; \
         rebuild the native crate with \
         `RUSTFLAGS=\"--cfg tokio_unstable\" cargo build -p datafusion-jni --features runtime-metrics` \
         to enable SessionContext.runtimeStats"
            .into(),
    )
}
