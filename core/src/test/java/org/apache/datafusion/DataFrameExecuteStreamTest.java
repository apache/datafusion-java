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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataFrameExecuteStreamTest {

  /**
   * Write a CSV with `rows` integer rows, one column `x`. Used in tests that need a real file scan
   * so DataFusion's batching honors {@code batch_size} -- in-memory {@code VALUES} plans get
   * coalesced into a single batch in some DataFusion versions, which would make those tests
   * brittle.
   */
  private static Path writeRowsCsv(Path dir, int rows) throws IOException {
    StringBuilder sb = new StringBuilder("x\n");
    for (int i = 1; i <= rows; i++) {
      sb.append(i).append('\n');
    }
    Path file = dir.resolve("rows.csv");
    Files.writeString(file, sb.toString());
    return file;
  }

  @Test
  void executeStreamYieldsTheSameRowsAsCollect() throws Exception {
    String sql = "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)";

    long collected = 0;
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(sql);
        ArrowReader reader = df.collect(allocator)) {
      while (reader.loadNextBatch()) {
        collected += reader.getVectorSchemaRoot().getRowCount();
      }
    }

    long streamed = 0;
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(sql);
        ArrowReader reader = df.executeStream(allocator)) {
      while (reader.loadNextBatch()) {
        streamed += reader.getVectorSchemaRoot().getRowCount();
      }
    }

    assertEquals(5L, collected);
    assertEquals(collected, streamed);
  }

  @Test
  void executeStreamConsumesTheDataFrame() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1");
      try (ArrowReader reader = df.executeStream(allocator)) {
        assertTrue(reader.loadNextBatch());
      }
      // After a successful executeStream, the DataFrame's native handle is
      // released. A second collect/executeStream/count must throw.
      assertThrows(IllegalStateException.class, () -> df.executeStream(allocator));
      assertThrows(IllegalStateException.class, () -> df.collect(allocator));
      assertThrows(IllegalStateException.class, df::count);
      // close() on an already-streamed DataFrame is a no-op (no double-free).
      df.close();
    }
  }

  @Test
  void executeStreamReadsBatchByBatch(@TempDir Path tempDir) throws Exception {
    // CSV with 5 rows scanned at batch_size=2 reliably yields multiple batches
    // across DataFusion versions, where an in-memory VALUES plan can be
    // coalesced into a single batch by the planner. The point of this test is
    // to pin "executeStream actually streams" without coupling to planner
    // batching behavior that may shift in upstream releases.
    Path csv = writeRowsCsv(tempDir, 5);
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().batchSize(2).build()) {
      ctx.registerCsv("rows", csv.toAbsolutePath().toString());
      try (DataFrame df = ctx.sql("SELECT x FROM rows");
          ArrowReader reader = df.executeStream(allocator)) {
        int batches = 0;
        long total = 0;
        int maxBatchSize = 0;
        while (reader.loadNextBatch()) {
          batches++;
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          total += root.getRowCount();
          maxBatchSize = Math.max(maxBatchSize, root.getRowCount());
        }
        assertEquals(5L, total);
        assertTrue(batches >= 2, "expected multiple batches with batchSize=2, got " + batches);
        assertTrue(maxBatchSize <= 2, "expected each batch <= 2 rows, got " + maxBatchSize);
      }
    }
  }

  @Test
  void executeStreamSurvivesEarlyClose() throws Exception {
    // Close the reader after the first batch and confirm no native panic /
    // resource leak. The DataFrame is already consumed; explicit close on it
    // must remain a no-op.
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().batchSize(1).build();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");
        ArrowReader reader = df.executeStream(allocator)) {
      assertTrue(reader.loadNextBatch());
    }
  }

  @Test
  void executeStreamOverParquetMatchesCollectRowCount() throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(
        Files.exists(lineitem), "TPC-H SF1 data not found; run `make tpch-data` first");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerParquet("lineitem", lineitem.toAbsolutePath().toString());

      long collected;
      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM lineitem");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector v = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        collected = v.get(0);
      }

      long streamed = 0;
      try (DataFrame df = ctx.sql("SELECT l_orderkey FROM lineitem");
          ArrowReader reader = df.executeStream(allocator)) {
        while (reader.loadNextBatch()) {
          streamed += reader.getVectorSchemaRoot().getRowCount();
        }
      }
      assertEquals(collected, streamed);
    }
  }

  @Test
  void executeStreamColumnValuesAreCorrect() throws Exception {
    // Pin actual cell values, not just row counts: a regression that
    // shipped wrong values per batch must be caught.
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().batchSize(2).build();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (10), (20), (30), (40)) AS t(x) ORDER BY x");
        ArrowReader reader = df.executeStream(allocator)) {
      java.util.List<Long> seen = new java.util.ArrayList<>();
      while (reader.loadNextBatch()) {
        BigIntVector v = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        for (int i = 0; i < v.getValueCount(); i++) {
          seen.add(v.get(i));
        }
      }
      assertEquals(java.util.List.of(10L, 20L, 30L, 40L), seen);
    }
  }
}
