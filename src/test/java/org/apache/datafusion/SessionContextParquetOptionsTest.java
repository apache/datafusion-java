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
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionContextParquetOptionsTest {

  @Test
  void readParquetWithDefaultOptionsCountsAllRows() throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(
        Files.exists(lineitem), "TPC-H SF1 data not found; run `make tpch-data` first");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readParquet(lineitem.toAbsolutePath().toString());
        ArrowReader reader = df.collect(allocator)) {
      long total = 0;
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        total += root.getRowCount();
      }
      assertEquals(6_001_215L, total);
      assertTrue(total > 0);
    }
  }

  @Test
  void registerParquetWithOptionsRespectsCustomExtension(@TempDir Path tempDir) throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(
        Files.exists(lineitem), "TPC-H SF1 data not found; run `make tpch-data` first");
    Path renamed = tempDir.resolve("lineitem.parq");
    Files.copy(lineitem, renamed, StandardCopyOption.REPLACE_EXISTING);

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerParquet(
          "t",
          renamed.toAbsolutePath().toString(),
          new ParquetReadOptions().fileExtension(".parq"));
      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM t");
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
  void readParquetWithExplicitSchemaUsesProvidedSchema() throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(
        Files.exists(lineitem), "TPC-H SF1 data not found; run `make tpch-data` first");

    Schema custom =
        new Schema(
            List.of(
                new Field("l_orderkey", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df =
            ctx.readParquet(
                lineitem.toAbsolutePath().toString(), new ParquetReadOptions().schema(custom));
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      Schema observed = reader.getVectorSchemaRoot().getSchema();
      assertEquals(1, observed.getFields().size());
      assertEquals("l_orderkey", observed.getFields().get(0).getName());
    }
  }

  @Test
  void registerParquetWithMetadataSizeHintIsAccepted() throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(
        Files.exists(lineitem), "TPC-H SF1 data not found; run `make tpch-data` first");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerParquet(
          "t",
          lineitem.toAbsolutePath().toString(),
          new ParquetReadOptions().metadataSizeHint(1L << 20));
      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector count = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(6_001_215L, count.get(0));
      }
    }
  }
}
