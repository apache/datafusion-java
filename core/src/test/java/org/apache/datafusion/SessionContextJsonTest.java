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
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionContextJsonTest {

  private static Path writeJson(Path dir, String name, String contents) throws IOException {
    Path file = dir.resolve(name);
    Files.writeString(file, contents);
    return file;
  }

  @Test
  void registerJsonInfersSchemaAndCounts(@TempDir Path tempDir) throws Exception {
    Path file =
        writeJson(
            tempDir,
            "people.json",
            "{\"id\":1,\"name\":\"alice\"}\n"
                + "{\"id\":2,\"name\":\"bob\"}\n"
                + "{\"id\":3,\"name\":\"carol\"}\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerJson("people", file.toAbsolutePath().toString());

      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM people");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector count = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(3L, count.get(0));
      }

      try (DataFrame df = ctx.sql("SELECT name FROM people WHERE id = 2");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        VarCharVector names = (VarCharVector) root.getVector(0);
        assertEquals("bob", new String(names.get(0)));
      }
    }
  }

  @Test
  void readJsonWithExplicitSchema(@TempDir Path tempDir) throws Exception {
    Path file =
        writeJson(
            tempDir, "headerless.json", "{\"id\":10,\"name\":\"x\"}\n{\"id\":20,\"name\":\"y\"}\n");

    Schema schema =
        new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df =
            ctx.readJson(file.toAbsolutePath().toString(), new NdJsonReadOptions().schema(schema));
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(2, root.getRowCount());
      assertEquals("id", root.getSchema().getFields().get(0).getName());
      assertEquals("name", root.getSchema().getFields().get(1).getName());
    }
  }

  @Test
  void registerJsonWithCustomExtension(@TempDir Path tempDir) throws Exception {
    Path file = writeJson(tempDir, "data.ndjson", "{\"x\":10,\"y\":20}\n{\"x\":30,\"y\":40}\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerJson(
          "t", file.toAbsolutePath().toString(), new NdJsonReadOptions().fileExtension(".ndjson"));

      try (DataFrame df = ctx.sql("SELECT SUM(x) + SUM(y) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector v = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(100L, v.get(0));
      }
    }
  }

  @Test
  void registerJsonRejectsNullArguments() {
    try (SessionContext ctx = new SessionContext()) {
      NdJsonReadOptions opts = new NdJsonReadOptions();
      assertThrows(IllegalArgumentException.class, () -> ctx.registerJson(null, "/p"));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerJson("t", null));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerJson(null, "/p", opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerJson("t", null, opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerJson("t", "/p", null));
    }
  }

  @Test
  void readJsonRejectsNullArguments() {
    try (SessionContext ctx = new SessionContext()) {
      NdJsonReadOptions opts = new NdJsonReadOptions();
      assertThrows(IllegalArgumentException.class, () -> ctx.readJson(null));
      assertThrows(IllegalArgumentException.class, () -> ctx.readJson(null, opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.readJson("/p", null));
    }
  }
}
