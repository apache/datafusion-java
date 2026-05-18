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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionContextAvroTest {

  /**
   * Write three rows of {@code (id long, name string)} as a single Avro object container file using
   * the canonical Apache Avro Java writer. Returns the path the test can hand to {@code
   * registerAvro} / {@code readAvro}.
   *
   * <p>Avro's logical types are minimal here on purpose: the goal is to pin that DataFusion's Avro
   * reader sees the file at all and that the JNI plumbing wires schema inference and SQL correctly.
   * Richer Avro type coverage is a job for upstream's own datasource tests.
   */
  private static Path writePeopleAvro(Path dir, String name) throws IOException {
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("Person")
            .namespace("org.apache.datafusion.test")
            .fields()
            .name("id")
            .type()
            .longType()
            .noDefault()
            .name("name")
            .type()
            .stringType()
            .noDefault()
            .endRecord();

    Path file = dir.resolve(name);
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(avroSchema, file.toFile());
      for (Object[] row :
          new Object[][] {
            {1L, "alice"}, {2L, "bob"}, {3L, "carol"},
          }) {
        GenericRecord rec = new GenericData.Record(avroSchema);
        rec.put("id", row[0]);
        rec.put("name", row[1]);
        writer.append(rec);
      }
    }
    return file;
  }

  @Test
  void registerAvroInfersSchemaAndCounts(@TempDir Path tempDir) throws Exception {
    Path file = writePeopleAvro(tempDir, "people.avro");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerAvro("people", file.toAbsolutePath().toString());

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
  void readAvroYieldsTheStoredRows(@TempDir Path tempDir) throws Exception {
    Path file = writePeopleAvro(tempDir, "people.avro");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readAvro(file.toAbsolutePath().toString());
        ArrowReader reader = df.collect(allocator)) {
      long total = 0;
      while (reader.loadNextBatch()) {
        total += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(3L, total);
    }
  }

  @Test
  void registerAvroWithCustomExtension(@TempDir Path tempDir) throws Exception {
    Path file = writePeopleAvro(tempDir, "people.av");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerAvro(
          "t", file.toAbsolutePath().toString(), new AvroReadOptions().fileExtension(".av"));

      try (DataFrame df = ctx.sql("SELECT SUM(id) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector sum = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(6L, sum.get(0));
      }
    }
  }

  @Test
  void readAvroWithExplicitSchemaIsAccepted(@TempDir Path tempDir) throws Exception {
    // Explicit schema overrides on-read inference. We supply the same schema the file actually
    // has, so query results stay correct; the test pins that the explicit-schema code path is
    // plumbed through and accepted (same as the Arrow / NDJSON readers).
    Path file = writePeopleAvro(tempDir, "people.avro");
    Schema schema =
        new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df =
            ctx.readAvro(file.toAbsolutePath().toString(), new AvroReadOptions().schema(schema));
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());
      assertEquals("id", root.getSchema().getFields().get(0).getName());
      assertEquals("name", root.getSchema().getFields().get(1).getName());
    }
  }

  @Test
  void registerAvroRejectsNullArguments() {
    try (SessionContext ctx = new SessionContext()) {
      AvroReadOptions opts = new AvroReadOptions();
      assertThrows(IllegalArgumentException.class, () -> ctx.registerAvro(null, "/p"));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerAvro("t", null));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerAvro(null, "/p", opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerAvro("t", null, opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerAvro("t", "/p", null));
    }
  }

  @Test
  void readAvroRejectsNullArguments() {
    try (SessionContext ctx = new SessionContext()) {
      AvroReadOptions opts = new AvroReadOptions();
      assertThrows(IllegalArgumentException.class, () -> ctx.readAvro(null));
      assertThrows(IllegalArgumentException.class, () -> ctx.readAvro(null, opts));
      assertThrows(IllegalArgumentException.class, () -> ctx.readAvro("/p", null));
    }
  }
}
