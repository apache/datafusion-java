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

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * A lazy representation of a query plan, mirroring the Rust DataFusion {@code DataFrame}. Created
 * by {@link SessionContext#sql(String)} or other planning entry points and executed by either
 * {@link #collect} (materializes every batch on the native heap before returning) or {@link
 * #executeStream} (yields one batch at a time as Java drains the reader).
 *
 * <p>Instances are <strong>not thread-safe</strong> and must be closed. Both {@link #collect} and
 * {@link #executeStream} consume the DataFrame: a successfully consumed DataFrame cannot be
 * consumed again by either method (or by other executors such as {@link #count}), and {@link
 * #close()} on an already-consumed instance is a no-op.
 */
public final class DataFrame implements AutoCloseable {
  static {
    NativeLibraryLoader.loadLibrary();
  }

  private long nativeHandle;

  DataFrame(long nativeHandle) {
    if (nativeHandle == 0) {
      throw new IllegalArgumentException("DataFrame native handle is null");
    }
    this.nativeHandle = nativeHandle;
  }

  /**
   * Execute the plan and return its record batches as an {@link ArrowReader}.
   *
   * <p>Consumes this DataFrame: the native plan is released as soon as the stream is established.
   * The caller is responsible for closing the returned reader, and the supplied allocator must
   * outlive it.
   *
   * <p>This method materializes every batch on the native heap before the first batch crosses the
   * FFI boundary, which can OOM the Rust side for unbounded or very large result sets. Prefer
   * {@link #executeStream(BufferAllocator)} for analytics-scale queries.
   */
  public ArrowReader collect(BufferAllocator allocator) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    long handle = nativeHandle;
    nativeHandle = 0;
    try {
      collectDataFrame(handle, stream.memoryAddress());
      return Data.importArrayStream(allocator, stream);
    } catch (Throwable e) {
      stream.close();
      throw e;
    }
  }

  /**
   * Execute the plan and return its record batches as a streaming {@link ArrowReader}. Each call to
   * {@link ArrowReader#loadNextBatch} drives one async {@code stream.next()} on the native side, so
   * memory pressure stays bounded by the executor pipeline plus one in-flight batch instead of the
   * full result set.
   *
   * <p>Consumes this DataFrame with the same lifecycle rules as {@link #collect(BufferAllocator)}:
   * the native plan is released as soon as the stream is established, the caller closes the
   * returned reader, and the supplied allocator must outlive it.
   *
   * <p>For result sets that fit comfortably in native memory and are read in their entirety, {@link
   * #collect(BufferAllocator)} remains a reasonable choice. For TB-scale or unbounded result sets,
   * use this method.
   */
  public ArrowReader executeStream(BufferAllocator allocator) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    long handle = nativeHandle;
    nativeHandle = 0;
    try {
      executeStreamDataFrame(handle, stream.memoryAddress());
      return Data.importArrayStream(allocator, stream);
    } catch (Throwable e) {
      stream.close();
      throw e;
    }
  }

  /** Execute the plan and return the number of rows. */
  public long count() {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return countRows(nativeHandle);
  }

  /** Execute the plan and print formatted batches to native stdout. */
  public void show() {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    showDataFrame(nativeHandle);
  }

  /** Execute the plan and print the first {@code limit} rows to native stdout. */
  public void show(int limit) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    showDataFrameWithLimit(nativeHandle, limit);
  }

  /**
   * Project the listed columns into a new DataFrame. The receiver remains usable and must still be
   * closed independently.
   */
  public DataFrame select(String... columnNames) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return new DataFrame(selectColumns(nativeHandle, columnNames));
  }

  /**
   * Apply a SQL predicate to produce a filtered DataFrame. The predicate is parsed against this
   * DataFrame's own schema. The receiver remains usable and must still be closed independently.
   */
  public DataFrame filter(String predicate) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return new DataFrame(filterRows(nativeHandle, predicate));
  }

  /**
   * Take the first {@code fetch} rows. Equivalent to {@link #limit(int, int)} with {@code skip =
   * 0}. The receiver remains usable and must still be closed independently.
   */
  public DataFrame limit(int fetch) {
    return limit(0, fetch);
  }

  /**
   * Skip {@code skip} rows, then take the next {@code fetch} rows. Both arguments must be
   * non-negative. The receiver remains usable and must still be closed independently.
   */
  public DataFrame limit(int skip, int fetch) {
    if (skip < 0) {
      throw new IllegalArgumentException("skip must be non-negative, was " + skip);
    }
    if (fetch < 0) {
      throw new IllegalArgumentException("fetch must be non-negative, was " + fetch);
    }
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return new DataFrame(limitRows(nativeHandle, skip, fetch));
  }

  /**
   * Deduplicate rows across all columns. The receiver remains usable and must still be closed
   * independently.
   */
  public DataFrame distinct() {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return new DataFrame(distinctRows(nativeHandle));
  }

  /**
   * Drop the named columns. The inverse of {@link #select(String...)}. The receiver remains usable
   * and must still be closed independently.
   */
  public DataFrame dropColumns(String... columnNames) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return new DataFrame(dropColumns(nativeHandle, columnNames));
  }

  /** Rename a column. The receiver remains usable and must still be closed independently. */
  public DataFrame withColumnRenamed(String oldName, String newName) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return new DataFrame(renameColumn(nativeHandle, oldName, newName));
  }

  /**
   * Add a column to this DataFrame computed from a SQL expression. If a column with the given name
   * already exists, it is replaced in place; otherwise the new column is appended. The expression
   * is parsed against this DataFrame's own schema, matching the convention used by {@link
   * #filter(String)}. The receiver remains usable and must still be closed independently.
   *
   * @throws IllegalArgumentException if {@code name} or {@code expr} is {@code null}.
   */
  public DataFrame withColumn(String name, String expr) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    if (name == null) {
      throw new IllegalArgumentException("withColumn name must be non-null");
    }
    if (expr == null) {
      throw new IllegalArgumentException("withColumn expr must be non-null");
    }
    return new DataFrame(withColumnExpr(nativeHandle, name, expr));
  }

  /**
   * Expand list or struct columns into rows or fields, with default {@link UnnestOptions} (i.e.
   * {@code preserveNulls = true}). The receiver remains usable and must still be closed
   * independently.
   */
  public DataFrame unnestColumns(String... columns) {
    return unnestColumns(new UnnestOptions(), columns);
  }

  /**
   * Expand list or struct columns into rows or fields with the supplied {@link UnnestOptions}. The
   * receiver remains usable and must still be closed independently.
   *
   * @throws IllegalArgumentException if {@code options} or {@code columns} is {@code null}.
   */
  public DataFrame unnestColumns(UnnestOptions options, String... columns) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    if (options == null) {
      throw new IllegalArgumentException("unnestColumns options must be non-null");
    }
    if (columns == null) {
      throw new IllegalArgumentException("unnestColumns columns must be non-null");
    }
    return new DataFrame(unnestColumns(nativeHandle, columns, options.preserveNulls()));
  }

  /**
   * Equi-join this DataFrame with {@code right} on the named columns, using the given {@link
   * JoinType}. The receiver and {@code right} both remain usable and must still be closed
   * independently.
   *
   * <p>Equivalent to SQL {@code left <type> JOIN right ON l.leftCols[0] = r.rightCols[0] AND ...}.
   * {@code leftCols} and {@code rightCols} must have the same length.
   *
   * @throws IllegalArgumentException if any argument is {@code null} or {@code leftCols.length !=
   *     rightCols.length}.
   * @throws IllegalStateException if either DataFrame is closed or already collected.
   * @throws RuntimeException if join planning fails (column collision in the combined schema,
   *     unknown column names, etc.).
   */
  public DataFrame join(DataFrame right, JoinType type, String[] leftCols, String[] rightCols) {
    checkJoinArgs(right, type, leftCols, rightCols);
    return new DataFrame(
        joinDataFrame(nativeHandle, right.nativeHandle, type.code(), leftCols, rightCols, null));
  }

  /**
   * Equi-join this DataFrame with {@code right}, restricting the result with a residual SQL filter
   * parsed against the <em>combined</em> schema (left columns followed by right columns; columns
   * may be qualified with the relation alias when ambiguous). The receiver and {@code right} both
   * remain usable and must still be closed independently.
   *
   * <p>For outer joins, the filter is applied only to matched rows; unmatched rows are passed
   * through with nulls on the unmatched side, matching DataFusion's semantics.
   *
   * @throws IllegalArgumentException if any argument is {@code null} or {@code leftCols.length !=
   *     rightCols.length}.
   * @throws IllegalStateException if either DataFrame is closed or already collected.
   * @throws RuntimeException if join planning or filter parsing fails.
   */
  public DataFrame join(
      DataFrame right, JoinType type, String[] leftCols, String[] rightCols, String filter) {
    checkJoinArgs(right, type, leftCols, rightCols);
    if (filter == null) {
      throw new IllegalArgumentException("join filter must be non-null");
    }
    return new DataFrame(
        joinDataFrame(nativeHandle, right.nativeHandle, type.code(), leftCols, rightCols, filter));
  }

  /**
   * Join this DataFrame with {@code right} using arbitrary SQL predicates parsed against the
   * <em>combined</em> schema. Each predicate is parsed independently and the join evaluates their
   * conjunction. Predicates may reference columns from either side and may be qualified with the
   * relation alias when ambiguous (e.g. {@code "left.x = right.x"}). The receiver and {@code right}
   * both remain usable and must still be closed independently.
   *
   * <p>DataFusion's optimiser identifies and rewrites equality predicates into hash-join keys
   * automatically, so {@code joinOn(right, INNER, "l.id = r.id")} plans equivalently to {@link
   * #join(DataFrame, JoinType, String[], String[])} with a single key. Use {@code joinOn} when the
   * predicate is not a simple equality, e.g. inequality joins or range conditions.
   *
   * @throws IllegalArgumentException if {@code right} or {@code type} is {@code null}, or {@code
   *     predicates} is {@code null} or empty, or any predicate is {@code null}.
   * @throws IllegalStateException if either DataFrame is closed or already collected.
   * @throws RuntimeException if predicate parsing or join planning fails.
   */
  public DataFrame joinOn(DataFrame right, JoinType type, String... predicates) {
    if (right == null) {
      throw new IllegalArgumentException("joinOn right must be non-null");
    }
    if (type == null) {
      throw new IllegalArgumentException("joinOn type must be non-null");
    }
    if (predicates == null || predicates.length == 0) {
      throw new IllegalArgumentException("joinOn predicates must be non-null and non-empty");
    }
    for (String p : predicates) {
      if (p == null) {
        throw new IllegalArgumentException("joinOn predicates must not contain null");
      }
    }
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    if (right.nativeHandle == 0) {
      throw new IllegalStateException("right DataFrame is closed or already collected");
    }
    return new DataFrame(
        joinOnDataFrame(nativeHandle, right.nativeHandle, type.code(), predicates));
  }

  private void checkJoinArgs(
      DataFrame right, JoinType type, String[] leftCols, String[] rightCols) {
    if (right == null) {
      throw new IllegalArgumentException("join right must be non-null");
    }
    if (type == null) {
      throw new IllegalArgumentException("join type must be non-null");
    }
    if (leftCols == null) {
      throw new IllegalArgumentException("join leftCols must be non-null");
    }
    if (rightCols == null) {
      throw new IllegalArgumentException("join rightCols must be non-null");
    }
    if (leftCols.length != rightCols.length) {
      throw new IllegalArgumentException(
          "join leftCols and rightCols must have the same length, got "
              + leftCols.length
              + " and "
              + rightCols.length);
    }
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    if (right.nativeHandle == 0) {
      throw new IllegalStateException("right DataFrame is closed or already collected");
    }
  }

  /**
   * Materialize this DataFrame as Parquet at {@code path}. The path is treated as a directory
   * unless overridden via {@link ParquetWriteOptions#singleFileOutput(boolean)}. The receiver
   * remains usable and must still be closed independently.
   *
   * @throws RuntimeException if the write fails.
   */
  public void writeParquet(String path) {
    writeParquet(path, new ParquetWriteOptions());
  }

  /**
   * Materialize this DataFrame as Parquet at {@code path} with the supplied {@link
   * ParquetWriteOptions}. The receiver remains usable and must still be closed independently.
   *
   * @throws RuntimeException if the write fails (path inaccessible, invalid compression spec,
   *     etc.).
   */
  public void writeParquet(String path, ParquetWriteOptions options) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    writeParquetWithOptions(
        nativeHandle,
        path,
        options.compression(),
        options.singleFileOutput() != null,
        options.singleFileOutput() != null && options.singleFileOutput());
  }

  /**
   * Materialize this DataFrame as CSV at {@code path}. The path is treated as a directory unless
   * overridden via {@link CsvWriteOptions#singleFileOutput(boolean)}. The receiver remains usable
   * and must still be closed independently.
   *
   * @throws RuntimeException if the write fails.
   */
  public void writeCsv(String path) {
    writeCsv(path, new CsvWriteOptions());
  }

  /**
   * Materialize this DataFrame as CSV at {@code path} with the supplied {@link CsvWriteOptions}.
   * The receiver remains usable and must still be closed independently.
   *
   * @throws IllegalArgumentException if {@code path} or {@code options} is {@code null}.
   * @throws RuntimeException if the write fails (path inaccessible, invalid compression spec,
   *     etc.).
   */
  public void writeCsv(String path, CsvWriteOptions options) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    if (path == null) {
      throw new IllegalArgumentException("writeCsv path must be non-null");
    }
    if (options == null) {
      throw new IllegalArgumentException("writeCsv options must be non-null");
    }
    writeCsvWithOptions(nativeHandle, path, options.toBytes());
  }

  /**
   * Materialize this DataFrame as newline-delimited JSON at {@code path}. The path is treated as a
   * directory unless overridden via {@link JsonWriteOptions#singleFileOutput(boolean)}. The
   * receiver remains usable and must still be closed independently.
   *
   * @throws RuntimeException if the write fails.
   */
  public void writeJson(String path) {
    writeJson(path, new JsonWriteOptions());
  }

  /**
   * Materialize this DataFrame as newline-delimited JSON at {@code path} with the supplied {@link
   * JsonWriteOptions}. The receiver remains usable and must still be closed independently.
   *
   * @throws IllegalArgumentException if {@code path} or {@code options} is {@code null}.
   * @throws RuntimeException if the write fails (path inaccessible, invalid compression spec,
   *     etc.).
   */
  public void writeJson(String path, JsonWriteOptions options) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    if (path == null) {
      throw new IllegalArgumentException("writeJson path must be non-null");
    }
    if (options == null) {
      throw new IllegalArgumentException("writeJson options must be non-null");
    }
    writeJsonWithOptions(nativeHandle, path, options.toBytes());
  }

  @Override
  public void close() {
    if (nativeHandle != 0) {
      closeDataFrame(nativeHandle);
      nativeHandle = 0;
    }
  }

  private static native void collectDataFrame(long handle, long ffiStreamAddr);

  private static native void executeStreamDataFrame(long handle, long ffiStreamAddr);

  private static native void closeDataFrame(long handle);

  private static native long countRows(long handle);

  private static native void showDataFrame(long handle);

  private static native void showDataFrameWithLimit(long handle, int limit);

  private static native long selectColumns(long handle, String[] columnNames);

  private static native long filterRows(long handle, String predicate);

  private static native long limitRows(long handle, int skip, int fetch);

  private static native long distinctRows(long handle);

  private static native long dropColumns(long handle, String[] columnNames);

  private static native long renameColumn(long handle, String oldName, String newName);

  private static native long withColumnExpr(long handle, String name, String expr);

  private static native long unnestColumns(long handle, String[] columns, boolean preserveNulls);

  private static native long joinDataFrame(
      long leftHandle,
      long rightHandle,
      byte joinType,
      String[] leftCols,
      String[] rightCols,
      String filter);

  private static native long joinOnDataFrame(
      long leftHandle, long rightHandle, byte joinType, String[] predicates);

  private static native void writeParquetWithOptions(
      long handle,
      String path,
      String compression,
      boolean singleFileOutputSet,
      boolean singleFileOutputValue);

  private static native void writeCsvWithOptions(long handle, String path, byte[] optionsBytes);

  private static native void writeJsonWithOptions(long handle, String path, byte[] optionsBytes);
}
