/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SessionContext#runtimeStats()}. The {@code runtime-metrics} Cargo feature is off
 * by default in {@code native/Cargo.toml}; if the native crate was built without it (and without
 * {@code RUSTFLAGS="--cfg tokio_unstable"}), every test here is skipped via {@link
 * #checkFeatureEnabled}. Run
 *
 * <pre>{@code
 * RUSTFLAGS="--cfg tokio_unstable" cargo build -p datafusion-jni --features runtime-metrics
 * }</pre>
 *
 * before {@code ./mvnw test} to exercise this class.
 */
class SessionContextRuntimeStatsTest {

  @BeforeAll
  static void checkFeatureEnabled() {
    try (SessionContext ctx = new SessionContext()) {
      ctx.runtimeStats();
    } catch (RuntimeException e) {
      String msg = e.getMessage() == null ? "" : e.getMessage();
      Assumptions.assumeFalse(
          msg.contains("runtime-metrics` Cargo feature"),
          "datafusion-jni built without the `runtime-metrics` feature; skipping runtime-stats"
              + " tests");
    }
  }

  @Test
  void fieldsAreNonNegative() {
    try (SessionContext ctx = new SessionContext()) {
      RuntimeStats s = ctx.runtimeStats();
      assertTrue(s.numWorkers() > 0, () -> "numWorkers=" + s.numWorkers());
      assertTrue(s.liveTasksCount() >= 0, () -> "liveTasksCount=" + s.liveTasksCount());
      assertTrue(s.globalQueueDepth() >= 0, () -> "globalQueueDepth=" + s.globalQueueDepth());
      assertTrue(s.elapsedNanos() >= 0L, () -> "elapsedNanos=" + s.elapsedNanos());
      assertTrue(s.totalBusyNanos() >= 0L, () -> "totalBusyNanos=" + s.totalBusyNanos());
      assertTrue(s.totalParkCount() >= 0L);
      assertTrue(s.totalPollsCount() >= 0L);
      assertTrue(s.totalNoopCount() >= 0L);
      assertTrue(s.totalStealCount() >= 0L);
      assertTrue(s.totalLocalScheduleCount() >= 0L);
      assertTrue(s.totalOverflowCount() >= 0L);
    }
  }

  @Test
  void numWorkersMatchesRuntimeShape() {
    try (SessionContext ctx = new SessionContext()) {
      // The shared multi-threaded Tokio runtime defaults to one worker per CPU.
      // We don't assert the exact count -- runners differ -- but it must be >= 1.
      assertTrue(ctx.runtimeStats().numWorkers() >= 1);
    }
  }

  @Test
  void elapsedIncreasesAcrossSnapshots() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      // The strict guarantee from `tokio_metrics::RuntimeMonitor::intervals()`
      // is that each delta covers the wall-clock since the previous probe.
      // `total_busy_duration` and `total_polls_count` only advance if work
      // *actually runs on a Tokio worker thread* during the interval -- and
      // for small in-memory DataFusion queries the future may complete
      // synchronously on the JNI thread (which is not a worker), so neither
      // metric is a reliable monotonic signal in tests. `elapsed` is, since
      // it's pure wall-clock between probes.
      RuntimeStats before = ctx.runtimeStats();
      for (int i = 0; i < 5; i++) {
        try (DataFrame df =
                ctx.sql(
                    "SELECT i FROM "
                        + "  (SELECT * FROM generate_series(1, 50000) AS t(i)) "
                        + "ORDER BY i DESC");
            ArrowReader reader = df.collect(allocator)) {
          while (reader.loadNextBatch()) {
            assertNotNull(reader.getVectorSchemaRoot());
          }
        }
      }
      RuntimeStats after = ctx.runtimeStats();
      assertTrue(
          after.elapsedNanos() > before.elapsedNanos(),
          () ->
              "after.elapsedNanos="
                  + after.elapsedNanos()
                  + " not greater than before="
                  + before.elapsedNanos());
    }
  }

  @Test
  void closedContextThrows() {
    SessionContext ctx = new SessionContext();
    ctx.close();
    assertThrows(IllegalStateException.class, ctx::runtimeStats);
  }
}
