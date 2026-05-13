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

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class SessionContextTest {
  @Test
  void canExecuteSelect1() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1");
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  void selectCountStarLineitem() throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(
        Files.exists(lineitem), "TPC-H SF1 data not found; run `make tpch-data` first");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerParquet("lineitem", lineitem.toAbsolutePath().toString());
      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM lineitem");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        BigIntVector count = (BigIntVector) root.getVector(0);
        assertEquals(6_001_215L, count.get(0));
      }
    }
  }

  @Test
  void dataFrameCloseWithoutCollectReleasesNativeHandle() {
    try (SessionContext ctx = new SessionContext()) {
      // Planning succeeds but we never execute. close() must release the native plan.
      DataFrame df = ctx.sql("SELECT 1");
      df.close();
      // Double-close must be a no-op.
      df.close();
    }
  }

  @Test
  void dataFrameCollectTwiceFails() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1")) {
      try (ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      assertThrows(IllegalStateException.class, () -> df.collect(allocator));
    }
  }
}
