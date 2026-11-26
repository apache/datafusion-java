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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.spark.AdbcInputPartition.Kind;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 * A planned ADBC-backed scan as a Spark {@link Scan}/{@link Batch}.
 *
 * <p>{@link #planInputPartitions()} runs on the driver and probes the driver's capabilities, since
 * the arrow-adbc JNI manager and the underlying driver implement different subsets of ADBC:
 *
 * <ol>
 *   <li>Pick the query wire: prefer a typed Substrait plan; if the driver reports {@code
 *       NOT_IMPLEMENTED} for {@code setSubstraitPlan}, fall back to a SQL string.
 *   <li>Pick the partitioning: try {@code executePartitioned()} for one descriptor per output
 *       partition; if {@code NOT_IMPLEMENTED}, emit a single partition carrying the chosen query.
 * </ol>
 *
 * No native handle ever crosses the wire -- each {@link AdbcInputPartition} carries only opaque
 * bytes, and each executor reopens its own ADBC connection.
 */
final class AdbcScanImpl implements Scan, Batch {

  private final StructType readSchema;
  private final AdbcOptions options;
  // null projection means all columns; column names in output order otherwise.
  private final List<String> projection;
  private final List<Filter> pushedFilters;
  private final OptionalLong limit;

  AdbcScanImpl(
      StructType readSchema,
      AdbcOptions options,
      List<String> projection,
      List<Filter> pushedFilters,
      OptionalLong limit) {
    this.readSchema = readSchema;
    this.options = options;
    this.projection = projection;
    this.pushedFilters = pushedFilters;
    this.limit = limit;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    try (BufferAllocator allocator = new RootAllocator();
        AdbcDatabase db = new JniDriver(allocator).open(options.driverParameters());
        AdbcConnection conn = db.connect()) {
      // Set DataFusion's target_partitions on the planning session. execute_partitions
      // pins it into each descriptor, so executors re-plan into the same partitioning.
      // Defaults to the cluster's parallelism (total executor cores). Repartition-aware
      // providers (e.g. file scans) use this to choose N; fixed-partition providers
      // (e.g. an in-memory table) keep their intrinsic partition count.
      int targetPartitions = options.targetPartitions().orElseGet(AdbcScanImpl::clusterParallelism);
      applyTargetPartitions(conn, targetPartitions);

      Schema arrow = conn.getTableSchema(null, null, options.table());

      // The casts (unsigned, Float16, non-µs timestamps, time) live only in the SQL projection, so
      // any schema needing one must use the SQL wire. The gate is the full schema, not just the
      // projection: the Substrait NamedScan declares every field's type, so an unprojected cast
      // column would still misdeclare the base schema. Substrait also can't encode several other
      // Spark-native Arrow types (binary, nested, decimal, ...); build() throws for those, which we
      // likewise treat as "not Substrait-representable" and fall back.
      boolean schemaNeedsCast = arrow.getFields().stream().anyMatch(SchemaConverter::needsCast);

      List<SchemaConverter.ProjectionColumn> columns =
          SchemaConverter.projectionColumns(arrow, projection);
      // count() (and other column-less reads) prunes the projection to empty. A bare SELECT *
      // would then return the raw, uncast schema and the reader would fail on a non-Spark-native
      // column, so when the table has any cast column, emit a single readable probe column
      // instead -- the row count is all such a scan needs.
      if (columns.isEmpty() && schemaNeedsCast) {
        columns = List.of(SchemaConverter.probeColumn(arrow));
      }
      boolean anyCast = columns.stream().anyMatch(c -> c.castType() != null);
      // SELECT * only when no columns are projected away and none need a cast; otherwise the
      // columns must be listed so the casts can be injected.
      List<SchemaConverter.ProjectionColumn> sqlColumns =
          (projection == null && !anyCast) ? null : columns;
      String sql = SqlQuery.build(options.table(), sqlColumns, pushedFilters, limit);

      byte[] substrait = null;
      if (!schemaNeedsCast) {
        try {
          substrait = SubstraitPlan.build(options.table(), arrow, projection, pushedFilters, limit);
        } catch (RuntimeException e) {
          substrait = null;
        }
      }
      return plan(conn, substrait, sql);
    } catch (Exception e) {
      throw new RuntimeException("failed to plan ADBC scan for table " + options.table(), e);
    }
  }

  private static void applyTargetPartitions(AdbcConnection conn, int targetPartitions)
      throws Exception {
    try (AdbcStatement stmt = conn.createStatement()) {
      stmt.setSqlQuery("SET datafusion.execution.target_partitions = " + targetPartitions);
      stmt.executeUpdate();
    }
  }

  /** Total executor cores via the active SparkSession; falls back to local cores. */
  private static int clusterParallelism() {
    try {
      return SparkSession.active().sparkContext().defaultParallelism();
    } catch (Throwable t) {
      return Runtime.getRuntime().availableProcessors();
    }
  }

  /** Probe the query wire and partitioning, returning the input partitions. */
  private InputPartition[] plan(AdbcConnection conn, byte[] substrait, String sql)
      throws Exception {
    Kind singleKind;
    byte[] singlePayload;

    // Force the SQL wire when Substrait can't encode this scan (null plan), or via the escape
    // hatch (e.g. when the Substrait round-trip plans to fewer partitions than SQL). Defaults to
    // preferring Substrait.
    boolean forceSql =
        substrait == null || "sql".equalsIgnoreCase(System.getProperty("adbc.wire", ""));

    AdbcStatement stmt = conn.createStatement();
    try {
      if (forceSql) {
        stmt.setSqlQuery(sql);
        singleKind = Kind.SQL;
        singlePayload = sql.getBytes(StandardCharsets.UTF_8);
      } else {
        try {
          stmt.setSubstraitPlan(directBuffer(substrait));
          singleKind = Kind.SUBSTRAIT;
          singlePayload = substrait;
        } catch (AdbcException e) {
          if (!notImplemented(e)) {
            throw e;
          }
          // Driver/JNI without Substrait support -> SQL.
          AutoCloseables.close(stmt);
          stmt = conn.createStatement();
          stmt.setSqlQuery(sql);
          singleKind = Kind.SQL;
          singlePayload = sql.getBytes(StandardCharsets.UTF_8);
        }
      }

      List<AdbcInputPartition> descriptors = tryPartition(stmt);
      if (descriptors != null) {
        return descriptors.toArray(new InputPartition[0]);
      }
    } finally {
      AutoCloseables.close(stmt);
    }
    // Single-partition fallback carrying the chosen query.
    return new InputPartition[] {new AdbcInputPartition(options, singlePayload, singleKind)};
  }

  /**
   * Attempt ADBC partitioned execution on the (already-query-set) statement. Returns one partition
   * per descriptor, or {@code null} if the driver does not implement partitioning.
   */
  private List<AdbcInputPartition> tryPartition(AdbcStatement stmt) throws Exception {
    try {
      AdbcStatement.PartitionResult result = stmt.executePartitioned();
      List<PartitionDescriptor> descriptors = result.getPartitionDescriptors();
      if (descriptors.isEmpty()) {
        return null;
      }
      List<AdbcInputPartition> partitions = new ArrayList<>(descriptors.size());
      for (PartitionDescriptor descriptor : descriptors) {
        partitions.add(
            new AdbcInputPartition(options, toBytes(descriptor.getDescriptor()), Kind.DESCRIPTOR));
      }
      return partitions;
    } catch (AdbcException e) {
      if (notImplemented(e)) {
        return null;
      }
      throw e;
    }
  }

  private static boolean notImplemented(AdbcException e) {
    return e.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED
        || e.getStatus() == AdbcStatusCode.NOT_FOUND;
  }

  private static ByteBuffer directBuffer(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes).flip();
    return buffer;
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    ByteBuffer dup = buffer.duplicate();
    byte[] bytes = new byte[dup.remaining()];
    dup.get(bytes);
    return bytes;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new AdbcPartitionReaderFactory();
  }
}
