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
    return collect(allocator, null);
  }

  /**
   * Execute the plan with cooperative cancellation. Identical to {@link #collect(BufferAllocator)}
   * except that {@link CancellationToken#cancel()} on {@code token} from another thread aborts the
   * call with a {@link java.util.concurrent.CancellationException} at the next poll point.
   *
   * <p>{@code token} may be {@code null}, in which case this overload behaves exactly like the
   * single-argument form.
   *
   * @throws java.util.concurrent.CancellationException if the token is fired during the call.
   */
  public ArrowReader collect(BufferAllocator allocator, CancellationToken token) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    long tokenHandle = resolveTokenHandle(token);
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    long handle = nativeHandle;
    nativeHandle = 0;
    try {
      collectDataFrame(handle, stream.memoryAddress(), tokenHandle);
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
    return executeStream(allocator, null);
  }

  /**
   * Execute the plan as a streaming reader with cooperative cancellation. Identical to {@link
   * #executeStream(BufferAllocator)} except that the returned reader holds the supplied {@code
   * token} for its full lifetime: firing the token from another thread aborts the next {@link
   * ArrowReader#loadNextBatch} call.
   *
   * <p>{@code token} may be {@code null}, in which case this overload behaves exactly like the
   * single-argument form.
   *
   * <p><b>Cancellation surface (read carefully).</b> The exception type depends on <em>when</em>
   * the cancel fires, because the Arrow C-data stream layer wraps any underlying error before it
   * reaches Java:
   *
   * <ul>
   *   <li>If the token is fired <b>before</b> the stream is established (e.g., the token was
   *       already cancelled at call time, or fires during plan compilation), this method throws
   *       {@link java.util.concurrent.CancellationException}.
   *   <li>If the token fires <b>after</b> {@code executeStream} returns, the next {@link
   *       ArrowReader#loadNextBatch} throws {@link java.io.IOException} whose message contains the
   *       string {@code "query cancelled"}. Callers that need to distinguish cancellation from real
   *       I/O failures must match on the message until a typed surface lands as a follow-up.
   * </ul>
   *
   * @throws java.util.concurrent.CancellationException if the token fires before the stream is
   *     established.
   */
  public ArrowReader executeStream(BufferAllocator allocator, CancellationToken token) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    long tokenHandle = resolveTokenHandle(token);
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    long handle = nativeHandle;
    nativeHandle = 0;
    try {
      executeStreamDataFrame(handle, stream.memoryAddress(), tokenHandle);
      return Data.importArrayStream(allocator, stream);
    } catch (Throwable e) {
      stream.close();
      throw e;
    }
  }

  /**
   * A {@code null} token disables cancellation; a non-null but already-closed token is rejected
   * with {@link IllegalStateException}, matching how {@link CancellationToken#cancel()} and {@link
   * CancellationToken#isCancelled()} behave on a closed token. Without this check, premature {@code
   * close()} on a token would silently fall back to an uncancellable call, which is hard to
   * diagnose.
   */
  private static long resolveTokenHandle(CancellationToken token) {
    if (token == null) {
      return 0L;
    }
    long handle = token.handle();
    if (handle == 0L) {
      throw new IllegalStateException("CancellationToken is closed");
    }
    return handle;
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

  @Override
  public void close() {
    if (nativeHandle != 0) {
      closeDataFrame(nativeHandle);
      nativeHandle = 0;
    }
  }

  private static native void collectDataFrame(long handle, long ffiStreamAddr, long tokenHandle);

  private static native void executeStreamDataFrame(
      long handle, long ffiStreamAddr, long tokenHandle);

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

  private static native void writeParquetWithOptions(
      long handle,
      String path,
      String compression,
      boolean singleFileOutputSet,
      boolean singleFileOutputValue);

  private static native void writeCsvWithOptions(long handle, String path, byte[] optionsBytes);
}
