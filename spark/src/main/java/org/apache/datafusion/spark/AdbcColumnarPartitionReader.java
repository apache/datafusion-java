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

package org.apache.datafusion.spark;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Reads one ADBC scan partition as Spark {@link ColumnarBatch}es, zero-copy.
 *
 * <p>This executor borrows a cached database from {@link AdbcConnectionPool} (cached per executor
 * so the DataFusion cdylib is loaded and its providers registered once per executor, not once per
 * task) and opens its own per-task connection off it, then obtains an {@link ArrowReader} one of
 * two ways depending on how the scan was planned (see {@link AdbcInputPartition}):
 *
 * <ul>
 *   <li>partition descriptor -> {@code AdbcConnection.readPartition(descriptor)} (multi-partition);
 *   <li>Substrait plan -> {@code setSubstraitPlan} + {@code executeQuery} (single-partition
 *       fallback).
 * </ul>
 *
 * <p>The imported Arrow vectors are wrapped directly in Spark {@link ArrowColumnVector}s -- no
 * per-cell copy -- which requires the executor JVM to have a single arrow-java (the cluster's Spark
 * Arrow), shared by the ADBC driver manager and Spark.
 *
 * <p>Lifecycle: the Arrow vectors are owned by the {@link ArrowReader}. We do not close the {@link
 * ColumnarBatch} (which would double-free the vectors); {@link #close()} closes the per-task ADBC
 * handles and releases the {@link AdbcConnectionPool.Lease}. The cached database and its root
 * allocator are owned by the pool and outlive this reader.
 */
final class AdbcColumnarPartitionReader implements PartitionReader<ColumnarBatch> {

  private final AdbcConnectionPool.Lease lease;
  // Non-null only on the executeQuery (single-partition fallback) path.
  private final AdbcStatement statement;
  private final AdbcStatement.QueryResult queryResult;
  private final ArrowReader reader;
  private final VectorSchemaRoot root;
  private final ColumnarBatch batch;

  AdbcColumnarPartitionReader(AdbcInputPartition partition) {
    AdbcConnectionPool.Lease ls = null;
    AdbcStatement stmt = null;
    AdbcStatement.QueryResult result = null;
    try {
      ls = AdbcConnectionPool.acquire(partition.options);
      AdbcConnection conn = ls.connection();

      ArrowReader r;
      switch (partition.kind) {
        case DESCRIPTOR -> {
          // Multi-partition path: read one opaque partition descriptor.
          r = conn.readPartition(directBuffer(partition.payload));
        }
        case SUBSTRAIT -> {
          stmt = conn.createStatement();
          stmt.setSubstraitPlan(directBuffer(partition.payload));
          result = stmt.executeQuery();
          r = result.getReader();
        }
        case SQL -> {
          stmt = conn.createStatement();
          stmt.setSqlQuery(new String(partition.payload, java.nio.charset.StandardCharsets.UTF_8));
          result = stmt.executeQuery();
          r = result.getReader();
        }
        default -> throw new IllegalStateException("unknown partition kind " + partition.kind);
      }

      this.lease = ls;
      this.statement = stmt;
      this.queryResult = result;
      this.reader = r;
      this.root = reader.getVectorSchemaRoot();
      this.batch = new ColumnarBatch(wrap(root));
    } catch (Exception e) {
      try {
        // Close only task-owned handles plus the lease; the lease leaves the cached
        // database/connection alone.
        AutoCloseables.close(result, stmt, ls);
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw new RuntimeException("failed to open ADBC scan partition", e);
    }
  }

  /** Wrap each Arrow vector of the (reused) root as a Spark column vector, once. */
  private static ColumnVector[] wrap(VectorSchemaRoot root) {
    ColumnVector[] columns = new ColumnVector[root.getFieldVectors().size()];
    int i = 0;
    for (FieldVector vector : root.getFieldVectors()) {
      columns[i++] = new ArrowColumnVector(vector);
    }
    return columns;
  }

  private static ByteBuffer directBuffer(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes).flip();
    return buffer;
  }

  @Override
  public boolean next() throws IOException {
    // The root's vectors are reloaded in place each batch; skip empty batches.
    while (reader.loadNextBatch()) {
      int rows = root.getRowCount();
      if (rows > 0) {
        batch.setNumRows(rows);
        return true;
      }
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return batch;
  }

  @Override
  public void close() throws IOException {
    try {
      // On the executeQuery path the QueryResult owns the reader, so close it instead
      // of the reader; on the readPartition path close the reader directly. The lease
      // releases the per-task child allocator and the per-task connection; the cached
      // database and root allocator are owned by the pool.
      AutoCloseable readerHandle = queryResult != null ? queryResult : reader;
      AutoCloseables.close(readerHandle, statement, lease);
    } catch (Exception e) {
      throw new IOException("failed to close ADBC scan partition", e);
    }
  }
}
