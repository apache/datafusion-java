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

import java.util.HashSet;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class DataFrameIntrospectionTest {
  @Test
  void schemaReturnsArrowSchema() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1 AS x, 'foo' AS s")) {
      Schema schema = df.schema();
      assertEquals(2, schema.getFields().size());
      assertEquals("x", schema.getFields().get(0).getName());
      assertTrue(schema.getFields().get(0).getType() instanceof ArrowType.Int);
      assertEquals("s", schema.getFields().get(1).getName());
      assertTrue(schema.getFields().get(1).getType() instanceof ArrowType.Utf8);
    }
  }

  @Test
  void schemaPreservesReceiver() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")) {
      Schema schema = df.schema();
      assertNotNull(schema);
      // Receiver still usable after schema().
      assertEquals(3L, df.count());
    }
  }

  @Test
  void schemaThrowsAfterClose() {
    try (SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1");
      df.close();
      assertThrows(IllegalStateException.class, df::schema);
    }
  }

  @Test
  void explainReturnsHumanReadablePlan() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT x FROM (VALUES (1), (2), (3)) AS t(x) WHERE x > 1");
        DataFrame explained = df.explain(false, false);
        ArrowReader reader = explained.collect(allocator)) {
      StringBuilder labels = new StringBuilder();
      StringBuilder bodies = new StringBuilder();
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // Column 0 = plan-type label, Column 1 = plan text.
        VarCharVector planType = (VarCharVector) root.getVector(0);
        VarCharVector planText = (VarCharVector) root.getVector(1);
        for (int i = 0; i < root.getRowCount(); i++) {
          labels.append(new String(planType.get(i))).append('\n');
          bodies.append(new String(planText.get(i))).append('\n');
        }
      }
      String labelsDump = labels.toString();
      String bodiesDump = bodies.toString();
      assertTrue(labelsDump.contains("logical_plan"), () -> "labels: " + labelsDump);
      assertTrue(bodiesDump.contains("Filter"), () -> "bodies: " + bodiesDump);
      assertTrue(bodiesDump.contains("Projection"), () -> "bodies: " + bodiesDump);
    }
  }

  @Test
  void explainVerboseAnalyzeProducesMoreRowsThanLazy() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      long lazyRows;
      try (DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2)) AS t(x)");
          DataFrame e = df.explain(false, false);
          ArrowReader reader = e.collect(allocator)) {
        lazyRows = countAllRows(reader);
      }
      long verboseRows;
      try (DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2)) AS t(x)");
          DataFrame e = df.explain(true, true);
          ArrowReader reader = e.collect(allocator)) {
        verboseRows = countAllRows(reader);
      }
      assertTrue(
          verboseRows > lazyRows,
          () -> "verbose+analyze rows " + verboseRows + " not greater than lazy " + lazyRows);
    }
  }

  @Test
  void explainPreservesReceiver() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2)) AS t(x)")) {
      try (DataFrame e = df.explain(false, false)) {
        // Just ensure it's a valid handle.
        assertNotNull(e.schema());
      }
      // Receiver still usable.
      assertEquals(2L, df.count());
    }
  }

  @Test
  void explainThrowsAfterClose() {
    try (SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1");
      df.close();
      assertThrows(IllegalStateException.class, () -> df.explain(false, false));
    }
  }

  @Test
  void cacheMaterializesPlan() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT x FROM (VALUES (1), (2), (3), (4)) AS t(x) WHERE x > 1");
        DataFrame cached = df.cache()) {
      assertEquals(3L, cached.count());
      // Schema is preserved after caching.
      Schema schema = cached.schema();
      assertEquals(1, schema.getFields().size());
      assertEquals("x", schema.getFields().get(0).getName());
    }
  }

  @Test
  void cachePreservesReceiver() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2)) AS t(x)")) {
      try (DataFrame cached = df.cache()) {
        assertEquals(2L, cached.count());
      }
      // Receiver still usable after cache().
      assertEquals(2L, df.count());
    }
  }

  @Test
  void cacheThrowsAfterClose() {
    try (SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1");
      df.close();
      assertThrows(IllegalStateException.class, df::cache);
    }
  }

  @Test
  void describeReturnsSummaryStats() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(x, s)");
        DataFrame stats = df.describe()) {
      // First column of describe() output is named "describe" and lists the statistic name.
      Schema schema = stats.schema();
      assertEquals("describe", schema.getFields().get(0).getName());

      Set<String> seen = new HashSet<>();
      try (ArrowReader reader = stats.collect(allocator)) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          VarCharVector labels = (VarCharVector) root.getVector(0);
          for (int i = 0; i < root.getRowCount(); i++) {
            seen.add(new String(labels.get(i)));
          }
        }
      }
      // DataFusion 53.1 reports these seven labels.
      assertTrue(seen.contains("count"), () -> "labels=" + seen);
      assertTrue(seen.contains("null_count"), () -> "labels=" + seen);
      assertTrue(seen.contains("mean"), () -> "labels=" + seen);
      assertTrue(seen.contains("std"), () -> "labels=" + seen);
      assertTrue(seen.contains("min"), () -> "labels=" + seen);
      assertTrue(seen.contains("max"), () -> "labels=" + seen);
      assertTrue(seen.contains("median"), () -> "labels=" + seen);
    }
  }

  @Test
  void describePreservesReceiver() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")) {
      try (DataFrame stats = df.describe()) {
        assertNotNull(stats.schema());
      }
      // Receiver still usable after describe().
      assertEquals(3L, df.count());
    }
  }

  @Test
  void describeThrowsAfterClose() {
    try (SessionContext ctx = new SessionContext()) {
      DataFrame df = ctx.sql("SELECT 1");
      df.close();
      assertThrows(IllegalStateException.class, df::describe);
    }
  }

  private static long countAllRows(ArrowReader reader) throws Exception {
    long n = 0;
    while (reader.loadNextBatch()) {
      n += reader.getVectorSchemaRoot().getRowCount();
    }
    return n;
  }
}
