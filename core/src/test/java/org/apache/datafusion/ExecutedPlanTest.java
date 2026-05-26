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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.protobuf.ExecutedPlanNodeProto;
import org.junit.jupiter.api.Test;

class ExecutedPlanTest {

  // ---- post-mortem contract --------------------------------------------

  @Test
  void executedPlanBeforeCollectThrows() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1")) {
      IllegalStateException ex = assertThrows(IllegalStateException.class, df::executedPlan);
      assertTrue(
          ex.getMessage() != null && ex.getMessage().contains("collect"),
          "expected error to mention collect(), got: " + ex.getMessage());
    }
  }

  @Test
  void executedPlanAfterCollectReturnsPopulatedMetrics() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator();
        DataFrame df = ctx.sql("SELECT 1")) {
      try (ArrowReader r = df.collect(allocator)) {
        while (r.loadNextBatch()) {
          // drain
        }
      }
      ExecutedPlan plan = df.executedPlan();
      assertNotNull(plan);
      assertNotNull(plan.name());
      assertFalse(plan.name().isEmpty(), "expected non-empty operator name");
      assertNotNull(plan.displayDetails());
      assertTrue(
          plan.metrics().outputRows().isPresent(), "expected outputRows to be present post-exec");
      assertEquals(
          1L, plan.metrics().outputRows().getAsLong(), "expected 1 output row from 'SELECT 1'");
    }
  }

  @Test
  void executedPlanAfterExecuteStreamReturnsPopulatedMetrics() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator();
        DataFrame df = ctx.sql("SELECT 1")) {
      try (ArrowReader r = df.executeStream(allocator)) {
        while (r.loadNextBatch()) {
          // drain
        }
      }
      ExecutedPlan plan = df.executedPlan();
      assertEquals(1L, plan.metrics().outputRows().getAsLong());
    }
  }

  @Test
  void executedPlanIsRepeatable() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator();
        DataFrame df = ctx.sql("SELECT 1")) {
      try (ArrowReader r = df.collect(allocator)) {
        while (r.loadNextBatch()) {
          // drain
        }
      }
      ExecutedPlan first = df.executedPlan();
      ExecutedPlan second = df.executedPlan();
      // The native side hands back a fresh tree each call, but the metric
      // values come from the same Arc<dyn ExecutionPlan>; two calls must
      // agree on every counter.
      assertEquals(first.name(), second.name());
      assertEquals(first.metrics().outputRows(), second.metrics().outputRows());
      assertEquals(first.children().size(), second.children().size());
    }
  }

  @Test
  void executedPlanOnClosedDataFrameThrows() {
    try (SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1");
      df.close();
      assertThrows(IllegalStateException.class, df::executedPlan);
    }
  }

  /**
   * Regression: a multi-partition execution must surface the {@code CoalescePartitionsExec}
   * upstream wraps the plan with at execute-time. If the registered Arc is the un-wrapped
   * pre-execution plan, the returned tree is missing the actual root operator and any metrics that
   * node accumulated. Targets a 2-partition plan via {@code targetPartitions(2)}; the planner
   * produces a multi-partition output for a SQL aggregate, and the executor wraps it before
   * draining.
   */
  @Test
  void multiPartitionExecutionIncludesCoalesceRoot() throws Exception {
    try (SessionContext ctx = SessionContext.builder().targetPartitions(2).build();
        BufferAllocator allocator = new RootAllocator();
        DataFrame df =
            ctx.sql(
                "SELECT x % 3 AS bucket, COUNT(*) AS n "
                    + "FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9)) AS t(x) "
                    + "GROUP BY bucket")) {
      try (ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          // drain
        }
      }
      ExecutedPlan plan = df.executedPlan();
      assertEquals(
          "CoalescePartitionsExec",
          plan.name(),
          "expected the executed root to be CoalescePartitionsExec for multi-partition execution; "
              + "got: "
              + plan.name());
      assertTrue(
          plan.metrics().outputRows().isPresent(), "expected coalesce root to track outputRows");
    }
  }

  /**
   * Regression: the executed-plan registry must NOT cross-contaminate when many DataFrames are
   * created in the same session. The native side keys the registry on a stable per-DataFrame id
   * (not the {@code Box<DataFrame>} pointer, which is freed during execute and may be reused by a
   * later allocation). If the keying ever regresses to "pointer integer", this test fails by
   * returning the second DataFrame's plan tree from the first DataFrame's handle.
   */
  @Test
  void manyDataFramesInOneSessionKeepDistinctPlans() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      // Each DataFrame produces a distinct number of output rows. A leak across the registry
      // would surface as the wrong outputRows on at least one of them.
      int[] rowCounts = {1, 7, 13, 5};
      DataFrame[] frames = new DataFrame[rowCounts.length];
      try {
        for (int i = 0; i < rowCounts.length; i++) {
          // Generate a SELECT that returns `rowCounts[i]` rows via VALUES.
          StringBuilder sql = new StringBuilder("SELECT * FROM (VALUES ");
          for (int r = 0; r < rowCounts[i]; r++) {
            if (r > 0) sql.append(',');
            sql.append('(').append(r).append(')');
          }
          sql.append(") AS t(x)");
          frames[i] = ctx.sql(sql.toString());
          try (ArrowReader reader = frames[i].collect(allocator)) {
            while (reader.loadNextBatch()) {
              // drain
            }
          }
        }
        for (int i = 0; i < rowCounts.length; i++) {
          ExecutedPlan plan = frames[i].executedPlan();
          assertEquals(
              rowCounts[i],
              plan.metrics().outputRows().orElse(-1L),
              "DataFrame "
                  + i
                  + " should report "
                  + rowCounts[i]
                  + " rows, not another df's count");
        }
      } finally {
        for (DataFrame f : frames) {
          if (f != null) f.close();
        }
      }
    }
  }

  // ---- codec round-trip ------------------------------------------------

  @Test
  void fromBytesRoundTripsEmptyTree() {
    ExecutedPlanNodeProto proto =
        ExecutedPlanNodeProto.newBuilder().setName("Root").setDisplayDetails("").build();
    ExecutedPlan plan = ExecutedPlan.fromBytes(proto.toByteArray());
    assertEquals("Root", plan.name());
    assertEquals(0, plan.children().size());
    assertTrue(plan.metrics().outputRows().isEmpty());
    assertTrue(plan.metrics().customCounters().isEmpty());
  }

  @Test
  void fromBytesPopulatesAllWellKnownMetrics() {
    ExecutedPlanNodeProto proto =
        ExecutedPlanNodeProto.newBuilder()
            .setName("MyExec")
            .setDisplayDetails("x=1")
            .setOutputRows(100L)
            .setElapsedComputeNanos(200_000L)
            .setOutputBytes(4096L)
            .setOutputBatches(5L)
            .setSpillCount(1L)
            .setSpilledBytes(8192L)
            .setSpilledRows(50L)
            .setCurrentMemoryUsage(16384L)
            .putCustomCounters("custom_a", 7L)
            .build();
    ExecutedPlan plan = ExecutedPlan.fromBytes(proto.toByteArray());
    OperatorMetrics m = plan.metrics();
    assertEquals(100L, m.outputRows().getAsLong());
    assertEquals(200_000L, m.elapsedComputeNanos().getAsLong());
    assertEquals(4096L, m.outputBytes().getAsLong());
    assertEquals(5L, m.outputBatches().getAsLong());
    assertEquals(1L, m.spillCount().getAsLong());
    assertEquals(8192L, m.spilledBytes().getAsLong());
    assertEquals(50L, m.spilledRows().getAsLong());
    assertEquals(16384L, m.currentMemoryUsage().getAsLong());
    assertEquals(7L, m.customCounters().get("custom_a"));
  }

  @Test
  void fromBytesRoundTripsChildrenInOrder() {
    ExecutedPlanNodeProto proto =
        ExecutedPlanNodeProto.newBuilder()
            .setName("Root")
            .setDisplayDetails("")
            .addChildren(ExecutedPlanNodeProto.newBuilder().setName("A").build())
            .addChildren(ExecutedPlanNodeProto.newBuilder().setName("B").build())
            .addChildren(ExecutedPlanNodeProto.newBuilder().setName("C").build())
            .build();
    ExecutedPlan plan = ExecutedPlan.fromBytes(proto.toByteArray());
    assertEquals(3, plan.children().size());
    assertEquals("A", plan.children().get(0).name());
    assertEquals("B", plan.children().get(1).name());
    assertEquals("C", plan.children().get(2).name());
  }
}
