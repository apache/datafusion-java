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

//! Session-wide memory accounting wrapper around any `MemoryPool`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::common::Result;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use jni::sys::jlong;

/// Wraps an inner `MemoryPool` with two counters: total bytes currently held
/// and the peak observed since the wrapper was constructed. Bookkeeping is two
/// atomic loads/stores per `grow`/`shrink` and adds no contention because the
/// inner pool already serializes its own state.
#[derive(Debug)]
pub struct TrackingMemoryPool {
    inner: Arc<dyn MemoryPool>,
    current_bytes: AtomicU64,
    peak_bytes: AtomicU64,
}

impl TrackingMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            current_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
        }
    }

    // Note: there are intentionally no separate `current_bytes()` /
    // `peak_bytes()` accessors. Reading them independently allows a torn
    // observation where `current > peak` while a concurrent `record_grow` is
    // mid-flight. Callers must use `snapshot()` instead, which clamps.

    /// Returns `(current_bytes, peak_bytes)` as a consistent snapshot. Two
    /// invariants are maintained:
    ///
    /// 1. **Within a snapshot:** `peak >= current`. This matters when a
    ///    concurrent `record_grow` is in-flight between its `fetch_add` of
    ///    `current_bytes` and its `fetch_max` of `peak_bytes` -- a naive
    ///    snapshot could observe the higher current but the stale peak.
    /// 2. **Across snapshots:** `peak` is monotonically non-decreasing for
    ///    every observer. If reader R1 ever sees peak = X, no later reader R2
    ///    (on any thread) sees peak < X.
    ///
    /// The read-side clamp `max(stored_peak, observed_current)` alone gives
    /// (1) but not (2): it's possible for R1 to *synthesize* a peak of X
    /// from a transiently-high current, and then for a `record_shrink` plus
    /// the slow path of `record_grow`'s missing `fetch_max` to leave the
    /// stored peak briefly below X while R2 polls. R2 would observe a
    /// smaller current and a smaller stored peak -- non-monotonic.
    ///
    /// The fix is to persist the clamped value back into `peak_bytes` via
    /// `fetch_max` *before* returning. Doing so guarantees every snapshot's
    /// returned peak is also reflected in the global atomic, so any
    /// subsequent observer's `peak_bytes.load()` is at least as high as the
    /// largest peak any earlier observer returned.
    ///
    /// Both atomics are `Relaxed`-ordered; we tolerate slightly stale values
    /// in exchange for cheaper writes.
    pub fn snapshot(&self) -> (u64, u64) {
        // Order matters on the load side too: read `peak` first, then
        // `current`. If a `record_grow` is in flight, this order ensures
        // that whenever we observe a fresh `current` we also observe at
        // least the corresponding (or older) `peak` -- so the eventual
        // clamp `max(peak, current)` produces the right value.
        let peak_observed = self.peak_bytes.load(Ordering::Relaxed);
        let current = self.current_bytes.load(Ordering::Relaxed);
        let clamped_peak = peak_observed.max(current);
        // Persist the clamped peak so future observers can never see a
        // smaller peak than the one we just returned. fetch_max is
        // monotonic, so this is a no-op when another thread (or this very
        // thread on a previous call) already pushed peak past clamped_peak.
        let stored_peak = self.peak_bytes.fetch_max(clamped_peak, Ordering::Relaxed);
        (current, stored_peak.max(clamped_peak))
    }

    fn record_grow(&self, additional: usize) {
        // We bump `current` first to avoid double-counting in `peak` if a
        // concurrent reader sees a stale current and a fresh peak. The
        // snapshot path's read-side `max(peak, current)` clamp closes the
        // window where `current` is up-to-date but `peak` is not.
        let now = self
            .current_bytes
            .fetch_add(additional as u64, Ordering::Relaxed)
            .saturating_add(additional as u64);
        // fetch_max is monotonic so concurrent grows still produce the right peak.
        self.peak_bytes.fetch_max(now, Ordering::Relaxed);
    }

    fn record_shrink(&self, shrink: usize) {
        // Saturating: if the inner pool ever calls shrink with a value larger
        // than the running total we'd rather report 0 than wrap around.
        let prev = self
            .current_bytes
            .fetch_sub(shrink as u64, Ordering::Relaxed);
        if prev < shrink as u64 {
            // Restore to zero. Concurrent grows that interleaved are fine --
            // they'll just see a transient under-count.
            self.current_bytes.store(0, Ordering::Relaxed);
        }
    }
}

impl MemoryPool for TrackingMemoryPool {
    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record_grow(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.record_shrink(shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record_grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Process-wide registry mapping a `SessionContext`'s native handle to its
/// `TrackingMemoryPool`. Populated at session creation, drained at session
/// close. We can't downcast `Arc<dyn MemoryPool>` back to the concrete type
/// without an `Any` bound (which the trait does not require), so instead we
/// keep a parallel `Arc` indexed by the JNI handle Java already threads
/// through every call.
fn registry() -> &'static Mutex<HashMap<jlong, Arc<TrackingMemoryPool>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<jlong, Arc<TrackingMemoryPool>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register(handle: jlong, pool: Arc<TrackingMemoryPool>) {
    registry()
        .lock()
        .expect("memory registry lock poisoned")
        .insert(handle, pool);
}

pub fn lookup(handle: jlong) -> Option<Arc<TrackingMemoryPool>> {
    registry()
        .lock()
        .expect("memory registry lock poisoned")
        .get(&handle)
        .cloned()
}

pub fn unregister(handle: jlong) {
    registry()
        .lock()
        .expect("memory registry lock poisoned")
        .remove(&handle);
}
