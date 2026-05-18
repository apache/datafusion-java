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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class DataSourceTest {

  private static final ArrowType INT32 = new ArrowType.Int(32, true);
  private static final ArrowType UTF8 = new ArrowType.Utf8();

  /**
   * In-memory {@link DataSource} fixture. The batches are serialised to Arrow IPC bytes once at
   * construction (using a private allocator); each {@link #scan(BufferAllocator)} call returns a
   * fresh {@link ArrowStreamReader} backed by those bytes, using the framework-supplied allocator.
   */
  static final class InMemoryDataSource implements DataSource {
    private final Schema schema;
    private final byte[] ipcBytes;
    private final AtomicInteger scanCount = new AtomicInteger();

    InMemoryDataSource(Schema schema, byte[] ipcBytes) {
      this.schema = schema;
      this.ipcBytes = ipcBytes;
    }

    /**
     * Build a fixture from one or more vector-schema-root batches. The caller's allocator may be a
     * temporary RootAllocator; this constructor reads all data into IPC bytes immediately.
     */
    static InMemoryDataSource fromBatches(Schema schema, List<VectorSchemaRoot> batches) {
      return new InMemoryDataSource(schema, serializeBatches(schema, batches));
    }

    static byte[] serializeBatches(Schema schema, List<VectorSchemaRoot> batches) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (BufferAllocator tmp = new RootAllocator();
          VectorSchemaRoot stagingRoot = VectorSchemaRoot.create(schema, tmp);
          ArrowStreamWriter writer =
              new ArrowStreamWriter(stagingRoot, null, Channels.newChannel(baos))) {
        writer.start();
        for (VectorSchemaRoot batch : batches) {
          stagingRoot.allocateNew();
          int rowCount = batch.getRowCount();
          stagingRoot.setRowCount(rowCount);
          for (int i = 0; i < batch.getFieldVectors().size(); i++) {
            org.apache.arrow.vector.FieldVector src = batch.getFieldVectors().get(i);
            org.apache.arrow.vector.FieldVector dst = stagingRoot.getFieldVectors().get(i);
            for (int r = 0; r < rowCount; r++) {
              dst.copyFromSafe(r, r, src);
            }
            dst.setValueCount(rowCount);
          }
          writer.writeBatch();
        }
        writer.end();
      } catch (IOException e) {
        throw new RuntimeException("failed to serialize batches", e);
      }
      return baos.toByteArray();
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ArrowReader scan(BufferAllocator allocator) {
      scanCount.incrementAndGet();
      return new ArrowStreamReader(new ByteArrayInputStream(ipcBytes), allocator);
    }

    int scanCount() {
      return scanCount.get();
    }
  }

  /** Build a one-batch in-memory fixture of (id INT, name UTF8) with the given rows. */
  private static InMemoryDataSource buildTwoColumnTable(int[] ids, String[] names) {
    Schema schema =
        new Schema(
            List.of(
                new Field("id", FieldType.nullable(INT32), null),
                new Field("name", FieldType.nullable(UTF8), null)));
    try (BufferAllocator tmp = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, tmp)) {
      IntVector idVec = (IntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      int n = ids.length;
      idVec.allocateNew(n);
      nameVec.allocateNew(n);
      for (int i = 0; i < n; i++) {
        idVec.set(i, ids[i]);
        nameVec.setSafe(i, names[i].getBytes());
      }
      idVec.setValueCount(n);
      nameVec.setValueCount(n);
      root.setRowCount(n);
      return InMemoryDataSource.fromBatches(schema, List.of(root));
    }
  }

  @Test
  void registerDataSource_selectStar_returnsAllRows() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      InMemoryDataSource src =
          buildTwoColumnTable(new int[] {1, 2, 3}, new String[] {"a", "b", "c"});
      ctx.registerDataSource("t", src);

      try (DataFrame df = ctx.sql("SELECT id, name FROM t ORDER BY id");
          ArrowReader r = df.collect(allocator)) {
        assertTrue(r.loadNextBatch());
        VectorSchemaRoot out = r.getVectorSchemaRoot();
        IntVector id = (IntVector) out.getVector("id");
        VarCharVector name = (VarCharVector) out.getVector("name");
        assertEquals(3, id.getValueCount());
        assertEquals(1, id.get(0));
        assertEquals(2, id.get(1));
        assertEquals(3, id.get(2));
        assertEquals("a", new String(name.get(0)));
        assertEquals("b", new String(name.get(1)));
        assertEquals("c", new String(name.get(2)));
        while (r.loadNextBatch()) {}
      }
      assertEquals(1, src.scanCount());
    }
  }

  @Test
  void registerDataSource_unionAllSelf_callsScanTwice() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      InMemoryDataSource src = buildTwoColumnTable(new int[] {1, 2}, new String[] {"a", "b"});
      ctx.registerDataSource("t", src);

      try (DataFrame df = ctx.sql("SELECT id FROM t UNION ALL SELECT id FROM t");
          ArrowReader r = df.collect(allocator)) {
        long total = 0;
        while (r.loadNextBatch()) {
          IntVector id = (IntVector) r.getVectorSchemaRoot().getVector("id");
          total += id.getValueCount();
        }
        assertEquals(4, total);
      }
      assertEquals(2, src.scanCount());
    }
  }
}
