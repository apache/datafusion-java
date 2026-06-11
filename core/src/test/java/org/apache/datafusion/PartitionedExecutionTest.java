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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

class PartitionedExecutionTest {

  /**
   * A plan whose physical form reliably keeps {@code targetPartitions} output partitions: the
   * hash-repartitioned GROUP BY can't be collapsed by the physical optimizer, unlike a bare
   * top-level round-robin repartition, which {@code EnforceDistribution} removes as non-beneficial.
   */
  private static final String GROUPED_SQL =
      "SELECT x FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) AS t(x) GROUP BY x";

  private static final List<Long> EXPECTED_ROWS = List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);

  /** Drain one partition's reader into a list of long values from column 0. */
  private static List<Long> drain(ArrowReader reader) throws Exception {
    List<Long> out = new ArrayList<>();
    while (reader.loadNextBatch()) {
      BigIntVector v = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
      for (int i = 0; i < v.getValueCount(); i++) {
        out.add(v.get(i));
      }
    }
    return out;
  }

  private static List<Long> drainPartition(
      PartitionedExecution exec, int partition, BufferAllocator allocator) throws Exception {
    try (ArrowReader reader = exec.executeStream(partition, allocator)) {
      return drain(reader);
    }
  }

  @Test
  void partitionCountMatchesTargetPartitions() throws Exception {
    try (SessionContext ctx = SessionContext.builder().targetPartitions(4).build();
        DataFrame df = ctx.sql(GROUPED_SQL);
        PartitionedExecution exec = df.toPartitionedExecution()) {
      assertEquals(4, exec.partitionCount());
    }
  }

  @Test
  void unionOfPartitionsEqualsFullResult() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().targetPartitions(4).build();
        DataFrame df = ctx.sql(GROUPED_SQL);
        PartitionedExecution exec = df.toPartitionedExecution()) {
      assertEquals(4, exec.partitionCount());
      List<Long> all = new ArrayList<>();
      for (int p = 0; p < exec.partitionCount(); p++) {
        all.addAll(drainPartition(exec, p, allocator));
      }
      all.sort(Long::compare);
      assertEquals(EXPECTED_ROWS, all);
    }
  }

  @Test
  void concurrentPartitionStreamsAreIndependent() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().targetPartitions(4).build();
        DataFrame df = ctx.sql(GROUPED_SQL);
        PartitionedExecution exec = df.toPartitionedExecution()) {
      int n = exec.partitionCount();
      assertEquals(4, n);
      ExecutorService pool = Executors.newFixedThreadPool(n);
      try {
        List<Callable<List<Long>>> jobs = new ArrayList<>();
        for (int p = 0; p < n; p++) {
          final int partition = p;
          jobs.add(() -> drainPartition(exec, partition, allocator));
        }
        List<Long> all = new ArrayList<>();
        for (Future<List<Long>> f : pool.invokeAll(jobs)) {
          all.addAll(f.get());
        }
        all.sort(Long::compare);
        assertEquals(EXPECTED_ROWS, all);
      } finally {
        pool.shutdownNow();
      }
    }
  }

  @Test
  void samePartitionCanBeStreamedTwiceOnStatelessScans() throws Exception {
    // Spark task retry / speculative execution re-executes a partition index.
    // Re-execution is only supported by plans whose partitions are stateless
    // scans (MemoryExec, table providers): a UNION ALL of two VALUES keeps one
    // re-executable MemoryExec partition per branch. Pipelines containing
    // RepartitionExec (e.g. a hash GROUP BY) panic on second execute -- its
    // per-partition channels are single-use -- which is why this test does not
    // reuse GROUPED_SQL.
    String unionSql =
        "SELECT * FROM (VALUES (1), (2)) AS t(x) UNION ALL SELECT * FROM (VALUES (3), (4)) AS t(x)";
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(unionSql);
        PartitionedExecution exec = df.toPartitionedExecution()) {
      assertEquals(2, exec.partitionCount());
      List<Long> firstTotal = new ArrayList<>();
      List<Long> secondTotal = new ArrayList<>();
      for (int p = 0; p < exec.partitionCount(); p++) {
        firstTotal.addAll(drainPartition(exec, p, allocator));
        secondTotal.addAll(drainPartition(exec, p, allocator));
      }
      firstTotal.sort(Long::compare);
      secondTotal.sort(Long::compare);
      assertEquals(List.of(1L, 2L, 3L, 4L), firstTotal);
      assertEquals(firstTotal, secondTotal);
    }
  }

  @Test
  void toPartitionedExecutionConsumesTheDataFrame() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1");
      try (PartitionedExecution exec = df.toPartitionedExecution()) {
        assertTrue(exec.partitionCount() >= 1);
      }
      assertThrows(IllegalStateException.class, () -> df.executeStream(allocator));
      assertThrows(IllegalStateException.class, df::toPartitionedExecution);
      // close() on a consumed DataFrame stays a no-op (no double-free).
      df.close();
    }
  }

  @Test
  void closeIsIdempotentAndBlocksFurtherUse() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1")) {
      PartitionedExecution exec = df.toPartitionedExecution();
      exec.close();
      exec.close();
      assertThrows(IllegalStateException.class, exec::partitionCount);
      assertThrows(IllegalStateException.class, () -> exec.executeStream(0, allocator));
    }
  }

  @Test
  void outOfRangePartitionThrowsClearError() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().targetPartitions(2).build();
        DataFrame df = ctx.sql(GROUPED_SQL);
        PartitionedExecution exec = df.toPartitionedExecution()) {
      assertEquals(2, exec.partitionCount());
      RuntimeException e =
          assertThrows(RuntimeException.class, () -> exec.executeStream(7, allocator));
      assertTrue(
          e.getMessage().contains("out of range"),
          "expected out-of-range message, got: " + e.getMessage());
      assertThrows(RuntimeException.class, () -> exec.executeStream(-1, allocator));
      // The handle survives a failed executeStream call.
      assertEquals(2, exec.partitionCount());
    }
  }
}
