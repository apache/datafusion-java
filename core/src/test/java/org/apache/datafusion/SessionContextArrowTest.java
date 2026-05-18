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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionContextArrowTest {

  /**
   * Write three rows of {@code (id INT, name UTF8)} as a single Arrow IPC file using arrow-vector's
   * built-in file writer. Returns the path the test can hand to {@code registerArrow} / {@code
   * readArrow}.
   */
  private static Path writePeopleArrow(Path dir, String name) throws IOException {
    Schema schema =
        new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null)));
    Path file = dir.resolve(name);
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector id = (IntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      id.allocateNew(3);
      nameVec.allocateNew(3);
      id.set(0, 1);
      id.set(1, 2);
      id.set(2, 3);
      nameVec.setSafe(0, "alice".getBytes());
      nameVec.setSafe(1, "bob".getBytes());
      nameVec.setSafe(2, "carol".getBytes());
      root.setRowCount(3);

      try (FileChannel ch =
              FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
          ArrowFileWriter writer = new ArrowFileWriter(root, null, ch)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
    }
    return file;
  }

  @Test
  void registerArrowInfersSchemaAndCounts(@TempDir Path tempDir) throws Exception {
    Path file = writePeopleArrow(tempDir, "people.arrow");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerArrow("people", file.toAbsolutePath().toString());

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
  void readArrowYieldsTheStoredRows(@TempDir Path tempDir) throws Exception {
    Path file = writePeopleArrow(tempDir, "people.arrow");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readArrow(file.toAbsolutePath().toString());
        ArrowReader reader = df.collect(allocator)) {
      long total = 0;
      while (reader.loadNextBatch()) {
        total += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(3L, total);
    }
  }

  @Test
  void registerArrowWithCustomExtension(@TempDir Path tempDir) throws Exception {
    Path file = writePeopleArrow(tempDir, "people.ipc");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerArrow(
          "t", file.toAbsolutePath().toString(), new ArrowReadOptions().fileExtension(".ipc"));

      try (DataFrame df = ctx.sql("SELECT SUM(id) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector sum = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(6L, sum.get(0));
      }
    }
  }

  @Test
  void readArrowWithExplicitSchemaIsAccepted(@TempDir Path tempDir) throws Exception {
    // Explicit schema overrides on-read inference. We supply the same schema the
    // file actually has, so query results stay correct; the test pins that the
    // explicit-schema code path is plumbed through and accepted.
    Path file = writePeopleArrow(tempDir, "people.arrow");
    Schema schema =
        new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null)));

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df =
            ctx.readArrow(file.toAbsolutePath().toString(), new ArrowReadOptions().schema(schema));
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());
      assertEquals("id", root.getSchema().getFields().get(0).getName());
      assertEquals("name", root.getSchema().getFields().get(1).getName());
    }
  }

  @Test
  void registerArrowRejectsNullArguments() {
    try (SessionContext ctx = new SessionContext()) {
      ArrowReadOptions opts = new ArrowReadOptions();
      assertThrows(IllegalArgumentException.class, () -> ctx.registerArrow(null, "/p"));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerArrow("t", null));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerArrow(null, "/p", opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerArrow("t", null, opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerArrow("t", "/p", null));
    }
  }

  @Test
  void readArrowRejectsNullArguments() {
    try (SessionContext ctx = new SessionContext()) {
      ArrowReadOptions opts = new ArrowReadOptions();
      assertThrows(IllegalArgumentException.class, () -> ctx.readArrow(null));
      assertThrows(IllegalArgumentException.class, () -> ctx.readArrow(null, opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.readArrow("/p", null));
    }
  }
}
