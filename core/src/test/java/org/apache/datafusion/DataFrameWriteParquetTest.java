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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

class DataFrameWriteParquetTest {

  private static final Path LINEITEM = Path.of("tpch-data/sf1/lineitem.parquet");
  private static final long LINEITEM_ROWS = 6_001_215L;

  private static void assumeLineitem() {
    Assumptions.assumeTrue(
        Files.exists(LINEITEM), "TPC-H SF1 data not found; run `make tpch-data` first");
  }

  private static long countRowsAt(Path path) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerParquet("t", path.toAbsolutePath().toString());
      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        return ((BigIntVector) root.getVector(0)).get(0);
      }
    }
  }

  @Test
  void writeParquetRoundTripsRowCount(@TempDir Path tempDir) throws Exception {
    assumeLineitem();
    Path out = tempDir.resolve("out");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readParquet(LINEITEM.toAbsolutePath().toString())) {
      df.writeParquet(out.toString());
    }

    assertEquals(LINEITEM_ROWS, countRowsAt(out));
  }

  @Test
  void writeParquetSingleFileProducesOneFile(@TempDir Path tempDir) throws Exception {
    assumeLineitem();
    Path out = tempDir.resolve("out.parquet");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readParquet(LINEITEM.toAbsolutePath().toString())) {
      df.writeParquet(out.toString(), new ParquetWriteOptions().singleFileOutput(true));
    }

    assertTrue(Files.isRegularFile(out), "expected single file at " + out);
    assertEquals(LINEITEM_ROWS, countRowsAt(out));
  }

  @Test
  void writeParquetWithCompressionRoundTrips(@TempDir Path tempDir) throws Exception {
    assumeLineitem();
    Path out = tempDir.resolve("zstd-out");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readParquet(LINEITEM.toAbsolutePath().toString())) {
      df.writeParquet(out.toString(), new ParquetWriteOptions().compression("zstd(3)"));
    }

    assertEquals(LINEITEM_ROWS, countRowsAt(out));
  }

  @Test
  void writeParquetRetainsDataFrame(@TempDir Path tempDir) throws Exception {
    assumeLineitem();
    Path out = tempDir.resolve("retained");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readParquet(LINEITEM.toAbsolutePath().toString())) {
      df.writeParquet(out.toString());
      assertEquals(LINEITEM_ROWS, df.count());
    }
  }
}
