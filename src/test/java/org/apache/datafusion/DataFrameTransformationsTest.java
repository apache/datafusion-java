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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

class DataFrameTransformationsTest {
  @Test
  void countReturnsRowCount() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")) {
      assertEquals(3L, df.count());
    }
  }

  @Test
  void showDoesNotThrow() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2)) AS t(x)")) {
      df.show();
    }
  }

  @Test
  void showWithLimitDoesNotThrow() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")) {
      df.show(0);
      df.show(1);
      df.show(1_000_000);
    }
  }

  @Test
  void selectProjectsAndReordersColumns() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame source = ctx.sql("SELECT 1 AS a, 2 AS b, 3 AS c");
        DataFrame projected = source.select("b", "a");
        ArrowReader reader = projected.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(1, root.getRowCount());
      assertArrayEquals(
          new String[] {"b", "a"},
          root.getSchema().getFields().stream().map(f -> f.getName()).toArray(String[]::new));
    }
  }

  @Test
  void selectIsNonDestructive() {
    try (SessionContext ctx = new SessionContext();
        DataFrame source = ctx.sql("SELECT 1 AS a, 2 AS b")) {
      try (DataFrame first = source.select("a")) {
        assertEquals(1L, first.count());
      }
      try (DataFrame second = source.select("b")) {
        assertEquals(1L, second.count());
      }
      assertEquals(1L, source.count());
    }
  }

  @Test
  void filterRemovesRows() {
    try (SessionContext ctx = new SessionContext();
        DataFrame source = ctx.sql("SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(x)");
        DataFrame filtered = source.filter("x > 2")) {
      assertEquals(2L, filtered.count());
    }
  }

  @Test
  void filterIsNonDestructive() {
    try (SessionContext ctx = new SessionContext();
        DataFrame source = ctx.sql("SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(x)")) {
      try (DataFrame filtered = source.filter("x > 2")) {
        assertEquals(2L, filtered.count());
      }
      assertEquals(4L, source.count());
    }
  }

  @Test
  void chainFilterSelectCount() {
    try (SessionContext ctx = new SessionContext();
        DataFrame source = ctx.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 10 AS a, 20 AS b");
        DataFrame chained = source.filter("a > 5").select("b")) {
      assertEquals(1L, chained.count());
    }
  }

  @Test
  void methodsThrowAfterClose() {
    try (SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1 AS x");
      df.close();
      assertThrows(IllegalStateException.class, () -> df.select("x"));
      assertThrows(IllegalStateException.class, () -> df.filter("x > 0"));
      assertThrows(IllegalStateException.class, df::count);
      assertThrows(IllegalStateException.class, df::show);
      assertThrows(IllegalStateException.class, () -> df.show(5));
    }
  }

  @Test
  void methodsThrowAfterCollect() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1 AS x")) {
      try (ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
      }
      assertThrows(IllegalStateException.class, () -> df.select("x"));
      assertThrows(IllegalStateException.class, () -> df.filter("x > 0"));
      assertThrows(IllegalStateException.class, df::count);
      assertThrows(IllegalStateException.class, df::show);
      assertThrows(IllegalStateException.class, () -> df.show(5));
    }
  }

  @Test
  void selectInvalidColumnThrows() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1 AS x")) {
      assertThrows(RuntimeException.class, () -> df.select("not_a_column"));
    }
  }

  @Test
  void filterMalformedPredicateThrows() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1 AS x")) {
      assertThrows(RuntimeException.class, () -> df.filter("this is not sql"));
    }
  }

  @Test
  void lineitemFilterCountAgainstSqlBaseline() throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(
        Files.exists(lineitem), "TPC-H SF1 data not found; run `make tpch-data` first");

    try (SessionContext ctx = new SessionContext()) {
      ctx.registerParquet("lineitem", lineitem.toAbsolutePath().toString());

      long viaDataFrame;
      try (DataFrame df = ctx.sql("SELECT * FROM lineitem");
          DataFrame filtered = df.filter("l_orderkey < 100")) {
        viaDataFrame = filtered.count();
      }

      long viaSql;
      try (BufferAllocator allocator = new RootAllocator();
          DataFrame df = ctx.sql("SELECT COUNT(*) FROM lineitem WHERE l_orderkey < 100");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        viaSql = ((BigIntVector) root.getVector(0)).get(0);
      }

      assertEquals(viaSql, viaDataFrame);
    }
  }
}
