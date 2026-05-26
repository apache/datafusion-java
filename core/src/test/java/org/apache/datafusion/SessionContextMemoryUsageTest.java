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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

class SessionContextMemoryUsageTest {

  /** A query with enough rows to drive a non-trivial sort, so peak memory is measurable. */
  private static final String HEAVY_SQL =
      "SELECT a, b FROM "
          + "  (SELECT i AS a, i * 7 AS b FROM "
          + "    (SELECT * FROM "
          + "      generate_series(1, 200000) AS t(i))) "
          + "ORDER BY b DESC";

  @Test
  void snapshotIsZeroBeforeAnyQuery() {
    try (SessionContext ctx = new SessionContext()) {
      MemoryUsage usage = ctx.memoryUsage();
      assertEquals(0L, usage.currentBytes());
      assertEquals(0L, usage.peakBytes());
    }
  }

  @Test
  void peakIncreasesAfterMemoryHeavyQuery() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      try (DataFrame df = ctx.sql(HEAVY_SQL);
          ArrowReader reader = df.collect(allocator)) {
        // Drain the result so the sort actually runs and allocates buffers.
        while (reader.loadNextBatch()) {
          assertNotNull(reader.getVectorSchemaRoot());
        }
      }
      MemoryUsage usage = ctx.memoryUsage();
      assertTrue(usage.peakBytes() > 0L, () -> "peakBytes was " + usage.peakBytes());
      assertTrue(
          usage.peakBytes() >= usage.currentBytes(),
          () -> "peak " + usage.peakBytes() + " < current " + usage.currentBytes());
    }
  }

  @Test
  void peakStaysAfterCurrentDrops() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      try (DataFrame df = ctx.sql(HEAVY_SQL);
          ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          assertNotNull(reader.getVectorSchemaRoot());
        }
      }
      MemoryUsage afterRun = ctx.memoryUsage();
      // After the query is fully drained and closed, current should be ~0
      // (DataFusion shrinks reservations on drop) but peak should remain.
      assertEquals(0L, afterRun.currentBytes());
      assertTrue(afterRun.peakBytes() > 0L);
    }
  }

  @Test
  void pollableFromAnotherThread() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      AtomicBoolean done = new AtomicBoolean(false);
      AtomicLong observedPeak = new AtomicLong(0L);
      AtomicReference<Throwable> error = new AtomicReference<>();

      Thread poller =
          new Thread(
              () -> {
                try {
                  while (!done.get()) {
                    long p = ctx.memoryUsage().peakBytes();
                    long prev = observedPeak.get();
                    if (p > prev) {
                      observedPeak.compareAndSet(prev, p);
                    }
                    Thread.yield();
                  }
                } catch (Throwable t) {
                  error.set(t);
                }
              });
      poller.start();
      try {
        try (DataFrame df = ctx.sql(HEAVY_SQL);
            ArrowReader reader = df.collect(allocator)) {
          while (reader.loadNextBatch()) {
            assertNotNull(reader.getVectorSchemaRoot());
          }
        }
      } finally {
        done.set(true);
        poller.join();
      }
      assertNotNull(observedPeak);
      // The poller should have observed at least one non-zero peak.
      assertTrue(
          observedPeak.get() > 0L,
          () -> "concurrent poller never saw non-zero peak; error=" + error.get());
      // And no exceptions on the polling thread.
      assertEquals(null, error.get());
    }
  }

  @Test
  void closedContextThrows() {
    SessionContext ctx = new SessionContext();
    ctx.close();
    assertThrows(IllegalStateException.class, ctx::memoryUsage);
  }
}
