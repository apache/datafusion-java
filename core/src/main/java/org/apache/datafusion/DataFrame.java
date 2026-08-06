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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.function.LongBinaryOperator;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A lazy representation of a query plan, mirroring the Rust DataFusion {@code DataFrame}. Created
 * by {@link SessionContext#sql(String)} or other planning entry points and executed by either
 * {@link #collect} (materializes every batch on the native heap before returning) or {@link
 * #executeStream} (yields one batch at a time as Java drains the reader).
 *
 * <p>Instances must be closed. Both {@link #collect} and {@link #executeStream} consume the
 * DataFrame: a successfully consumed DataFrame cannot be consumed again by either method (or by
 * other executors such as {@link #count}), and {@link #close()} on an already-consumed instance is
 * a no-op.
 *
 * <p>Instances are safe to share between threads. This class pins its native handle for the
 * duration of every call, so a {@link #close} or a consuming operation racing with work on another
 * thread cannot free the plan out from under it; the call that loses such a race throws {@link
 * IllegalStateException}. When several threads race to consume the same DataFrame, exactly one
 * succeeds. {@link #close} and the consuming operations block until calls already in flight have
 * returned.
 */
public final class DataFrame implements AutoCloseable {
  static {
    NativeLibraryLoader.loadLibrary();
  }

  private static final String CLOSED = "DataFrame is closed or already collected";

  private final NativeHandle handle;

  DataFrame(long nativeHandle) {
    if (nativeHandle == 0) {
      throw new IllegalArgumentException("DataFrame native handle is null");
    }
    this.handle = new NativeHandle(nativeHandle, CLOSED);
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
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    try {
      collectDataFrame(handle.claim(), stream.memoryAddress());
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
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    try {
      executeStreamDataFrame(handle.claim(), stream.memoryAddress());
      return Data.importArrayStream(allocator, stream);
    } catch (Throwable e) {
      stream.close();
      throw e;
    }
  }

  /**
   * Return the Arrow {@link Schema} of this DataFrame's output. Non-consuming: the receiver remains
   * usable and must still be closed independently. Schema inspection does not execute the plan.
   *
   * <p>The schema is transferred via Arrow IPC; no {@link BufferAllocator} is required because a
   * schema carries no buffer data.
   */
  public Schema schema() {
    byte[] ipcBytes;
    long h = handle.acquire();
    try {
      ipcBytes = schemaIpc(h);
    } finally {
      handle.release();
    }
    try {
      return MessageSerializer.deserializeSchema(
          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(ipcBytes))));
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize IPC schema", e);
    }
  }

  /**
   * Return a new DataFrame whose rows describe the plan that would execute this DataFrame.
   * Non-consuming: the receiver remains usable and must still be closed independently.
   *
   * <p>With {@code verbose=false} and {@code analyze=false} (the cheap, lazy variant), the result
   * contains the logical plan only. {@code verbose=true} adds optimised-plan and physical-plan
   * rows; {@code analyze=true} runs the plan and attaches per-operator metrics. Render via {@link
   * #show()} or {@link #collect(BufferAllocator)}.
   */
  public DataFrame explain(boolean verbose, boolean analyze) {
    long h = handle.acquire();
    try {
      return new DataFrame(explainPlan(h, verbose, analyze));
    } finally {
      handle.release();
    }
  }

  /**
   * Materialise this DataFrame into an in-memory table and return a new DataFrame that scans it.
   * Non-consuming: the receiver remains usable and must still be closed independently.
   *
   * <p>Executes the plan eagerly: the entire result set is held in native memory until the returned
   * DataFrame is closed. Suitable for intermediate results that will be reused across multiple
   * downstream queries.
   *
   * @throws RuntimeException if execution fails.
   */
  public DataFrame cache() {
    long h = handle.acquire();
    try {
      return new DataFrame(cachePlan(h));
    } finally {
      handle.release();
    }
  }

  /**
   * Compute summary statistics (count, null_count, mean, std, min, max, median) over this
   * DataFrame's columns and return them as a new DataFrame. Non-consuming: the receiver remains
   * usable and must still be closed independently.
   *
   * <p>Executes the plan: DataFusion runs seven aggregate sub-plans against this DataFrame to build
   * the summary table. Numeric columns receive every statistic; non-numeric columns receive {@code
   * count} / {@code null_count} / {@code min} / {@code max} where applicable.
   *
   * @throws RuntimeException if execution fails.
   */
  public DataFrame describe() {
    long h = handle.acquire();
    try {
      return new DataFrame(describePlan(h));
    } finally {
      handle.release();
    }
  }

  /** Execute the plan and return the number of rows. */
  public long count() {
    long h = handle.acquire();
    try {
      return countRows(h);
    } finally {
      handle.release();
    }
  }

  /** Execute the plan and print formatted batches to native stdout. */
  public void show() {
    long h = handle.acquire();
    try {
      showDataFrame(h);
    } finally {
      handle.release();
    }
  }

  /** Execute the plan and print the first {@code limit} rows to native stdout. */
  public void show(int limit) {
    long h = handle.acquire();
    try {
      showDataFrameWithLimit(h, limit);
    } finally {
      handle.release();
    }
  }

  /**
   * Project the listed columns into a new DataFrame. The receiver remains usable and must still be
   * closed independently.
   */
  public DataFrame select(String... columnNames) {
    long h = handle.acquire();
    try {
      return new DataFrame(selectColumns(h, columnNames));
    } finally {
      handle.release();
    }
  }

  /**
   * Apply a SQL predicate to produce a filtered DataFrame. The predicate is parsed against this
   * DataFrame's own schema. The receiver remains usable and must still be closed independently.
   */
  public DataFrame filter(String predicate) {
    long h = handle.acquire();
    try {
      return new DataFrame(filterRows(h, predicate));
    } finally {
      handle.release();
    }
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
    long h = handle.acquire();
    try {
      return new DataFrame(limitRows(h, skip, fetch));
    } finally {
      handle.release();
    }
  }

  /**
   * Deduplicate rows across all columns. The receiver remains usable and must still be closed
   * independently.
   */
  public DataFrame distinct() {
    long h = handle.acquire();
    try {
      return new DataFrame(distinctRows(h));
    } finally {
      handle.release();
    }
  }

  /**
   * Drop the named columns. The inverse of {@link #select(String...)}. The receiver remains usable
   * and must still be closed independently.
   */
  public DataFrame dropColumns(String... columnNames) {
    long h = handle.acquire();
    try {
      return new DataFrame(dropColumns(h, columnNames));
    } finally {
      handle.release();
    }
  }

  /** Rename a column. The receiver remains usable and must still be closed independently. */
  public DataFrame withColumnRenamed(String oldName, String newName) {
    long h = handle.acquire();
    try {
      return new DataFrame(renameColumn(h, oldName, newName));
    } finally {
      handle.release();
    }
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
    long h = handle.acquire();
    try {
      if (name == null) {
        throw new IllegalArgumentException("withColumn name must be non-null");
      }
      if (expr == null) {
        throw new IllegalArgumentException("withColumn expr must be non-null");
      }
      return new DataFrame(withColumnExpr(h, name, expr));
    } finally {
      handle.release();
    }
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
    long h = handle.acquire();
    try {
      if (options == null) {
        throw new IllegalArgumentException("unnestColumns options must be non-null");
      }
      if (columns == null) {
        throw new IllegalArgumentException("unnestColumns columns must be non-null");
      }
      return new DataFrame(unnestColumns(h, columns, options.preserveNulls()));
    } finally {
      handle.release();
    }
  }

  // -- Set operations ------------------------------------------------------
  //
  // The Java method names mirror DataFusion's Rust API verbatim. SQL semantics:
  //
  //   union               = UNION ALL  (positional, keeps duplicates)
  //   unionDistinct       = UNION      (positional, deduplicated)
  //   unionByName         = UNION ALL  by column name; missing columns become NULL
  //   unionByNameDistinct = UNION      by column name; missing columns become NULL
  //   intersect           = INTERSECT ALL (keeps duplicates)
  //   intersectDistinct   = INTERSECT     (deduplicated)
  //   except              = EXCEPT ALL    (keeps duplicates)
  //   exceptDistinct      = EXCEPT        (deduplicated)
  //
  // Note: the *_distinct variants deduplicate, while the unsuffixed methods keep
  // duplicates. This is the inverse of Spark's convention, where `intersect`
  // deduplicates and `intersectAll` keeps duplicates -- consult the Javadoc on
  // each method to confirm semantics before porting Spark code.
  //
  // None of these methods consume the receiver or {@code other}; both DataFrames
  // remain usable after the call. The native side clones the LogicalPlan on
  // each side, which is cheap (LogicalPlan is Arc-backed in DataFusion).

  /**
   * Concatenate this DataFrame with {@code other} by column position, keeping all duplicates (SQL
   * {@code UNION ALL}). The two schemas must match positionally. Both this DataFrame and {@code
   * other} remain usable after the call and must still be closed independently.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if the schemas are incompatible.
   */
  public DataFrame union(DataFrame other) {
    return combine("union", other, DataFrame::unionRows);
  }

  /**
   * Concatenate this DataFrame with {@code other} by column position, removing duplicates (SQL
   * {@code UNION DISTINCT} -- equivalent to plain {@code UNION} in standard SQL). Both DataFrames
   * remain usable.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if the schemas are incompatible.
   */
  public DataFrame unionDistinct(DataFrame other) {
    return combine("unionDistinct", other, DataFrame::unionDistinctRows);
  }

  /**
   * Concatenate this DataFrame with {@code other} by column name, keeping all duplicates. Columns
   * present in only one side are filled with NULL on the other. Both DataFrames remain usable.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if column types disagree on a shared name.
   */
  public DataFrame unionByName(DataFrame other) {
    return combine("unionByName", other, DataFrame::unionByNameRows);
  }

  /**
   * Concatenate this DataFrame with {@code other} by column name, removing duplicates. Columns
   * present in only one side are filled with NULL on the other. Both DataFrames remain usable.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if column types disagree on a shared name.
   */
  public DataFrame unionByNameDistinct(DataFrame other) {
    return combine("unionByNameDistinct", other, DataFrame::unionByNameDistinctRows);
  }

  /**
   * Rows present in both this DataFrame and {@code other}, keeping duplicates from the receiver
   * (SQL {@code INTERSECT ALL}). Both schemas must match positionally. Both DataFrames remain
   * usable.
   *
   * <p><strong>Implementation note:</strong> DataFusion implements {@code INTERSECT ALL} as a
   * left-semi join on equality, not as standard SQL bag intersection. A left row is kept iff any
   * matching row exists in {@code other}. With {@code left = (1, 2, 2, 3)} and {@code right = (2,
   * 3)}, the result is {@code (2, 2, 3)} -- both copies of {@code 2} survive because each finds a
   * match in {@code right}. PostgreSQL / Spark {@code INTERSECT ALL} would also yield {@code (2, 2,
   * 3)} here, but the two engines diverge when {@code other} has fewer copies than {@code this} of
   * a row that appears in both.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if the schemas are incompatible.
   */
  public DataFrame intersect(DataFrame other) {
    return combine("intersect", other, DataFrame::intersectRows);
  }

  /**
   * Rows present in both this DataFrame and {@code other}, deduplicated (SQL {@code INTERSECT}).
   * Both schemas must match positionally. Both DataFrames remain usable.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if the schemas are incompatible.
   */
  public DataFrame intersectDistinct(DataFrame other) {
    return combine("intersectDistinct", other, DataFrame::intersectDistinctRows);
  }

  /**
   * Rows present in this DataFrame but not in {@code other}, keeping duplicates from the receiver
   * (SQL {@code EXCEPT ALL}). Both schemas must match positionally. Both DataFrames remain usable.
   *
   * <p><strong>Implementation note:</strong> DataFusion implements {@code EXCEPT ALL} as a
   * left-anti join on equality, not as standard SQL bag difference. A left row is kept iff
   * <em>no</em> matching row exists in {@code other} -- the multiplicity of matches is irrelevant.
   * With {@code left = (1, 1, 2, 2, 3)} and {@code right = (1, 3)}, the result is {@code (2, 2)}:
   * both copies of {@code 2} survive (no match in {@code right}); both copies of {@code 1} and the
   * {@code 3} drop. PostgreSQL / Spark {@code EXCEPT ALL} would yield the same answer here, but the
   * two engines diverge when {@code right} contains fewer copies than {@code left} of a row that
   * appears in both.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if the schemas are incompatible.
   */
  public DataFrame except(DataFrame other) {
    return combine("except", other, DataFrame::exceptRows);
  }

  /**
   * Rows present in this DataFrame but not in {@code other}, deduplicated (SQL {@code EXCEPT}).
   * Both schemas must match positionally. Both DataFrames remain usable.
   *
   * @throws IllegalArgumentException if {@code other} is {@code null}.
   * @throws RuntimeException if the schemas are incompatible.
   */
  public DataFrame exceptDistinct(DataFrame other) {
    return combine("exceptDistinct", other, DataFrame::exceptDistinctRows);
  }

  /**
   * Validate the receiver and {@code other}, pin both handles, and apply a two-handle native set
   * operation. Common body for the eight set-operation methods so the validation and pinning stay
   * in one place.
   *
   * <p>Pinning two handles cannot deadlock: {@link NativeHandle#acquire()} never blocks, so a
   * thread holding the receiver's pin is never waiting on the other DataFrame's lifetime.
   */
  private DataFrame combine(String op, DataFrame other, LongBinaryOperator nativeOp) {
    long left = handle.acquire();
    try {
      if (other == null) {
        throw new IllegalArgumentException(op + " other must be non-null");
      }
      long right = other.acquireAsOperand(op + " other DataFrame is closed or already collected");
      try {
        return new DataFrame(nativeOp.applyAsLong(left, right));
      } finally {
        other.handle.release();
      }
    } finally {
      handle.release();
    }
  }

  /**
   * Pin this DataFrame as the non-receiver operand of a binary operation, reporting a closed handle
   * with {@code message} so the caller can distinguish which side was already gone.
   */
  private long acquireAsOperand(String message) {
    try {
      return handle.acquire();
    } catch (IllegalStateException e) {
      throw new IllegalStateException(message);
    }
  }

  /**
   * Order the rows by the supplied sort keys. Each {@link SortExpr} names a column and a direction
   * ({@link SortExpr#asc(String)} / {@link SortExpr#desc(String)}); call {@link
   * SortExpr#nullsFirst(boolean)} to override null placement.
   *
   * <p>An empty {@code exprs} array is a no-op (matches DataFusion's {@code sort(vec![])}). The
   * receiver remains usable and must still be closed independently.
   *
   * @throws IllegalArgumentException if {@code exprs} or any element is {@code null}.
   * @throws RuntimeException if a sort column does not exist in this DataFrame's schema.
   */
  public DataFrame sort(SortExpr... exprs) {
    long h = handle.acquire();
    try {
      if (exprs == null) {
        throw new IllegalArgumentException("sort exprs must be non-null");
      }
      String[] columns = new String[exprs.length];
      boolean[] ascending = new boolean[exprs.length];
      boolean[] nullsFirst = new boolean[exprs.length];
      for (int i = 0; i < exprs.length; i++) {
        SortExpr e = exprs[i];
        if (e == null) {
          throw new IllegalArgumentException("sort exprs[" + i + "] must be non-null");
        }
        columns[i] = e.column();
        ascending[i] = e.ascending();
        nullsFirst[i] = e.nullsFirst();
      }
      return new DataFrame(sortRows(h, columns, ascending, nullsFirst));
    } finally {
      handle.release();
    }
  }

  /**
   * Repartition this DataFrame using a round-robin scheme across {@code numPartitions} output
   * partitions. The receiver remains usable and must still be closed independently.
   *
   * @throws IllegalArgumentException if {@code numPartitions <= 0}.
   * @throws RuntimeException if the underlying repartition plan rejects the request.
   */
  public DataFrame repartitionRoundRobin(int numPartitions) {
    long h = handle.acquire();
    try {
      if (numPartitions <= 0) {
        throw new IllegalArgumentException("numPartitions must be positive, was " + numPartitions);
      }
      return new DataFrame(repartitionRoundRobinRows(h, numPartitions));
    } finally {
      handle.release();
    }
  }

  /**
   * Repartition this DataFrame by hashing the named columns into {@code numPartitions} output
   * partitions. v1 supports column-name keys only; expression keys are deferred until the Java
   * binding gains an {@code Expr} builder. The receiver remains usable and must still be closed
   * independently.
   *
   * @throws IllegalArgumentException if {@code numPartitions <= 0}, {@code columns} is {@code null}
   *     or empty, or any element of {@code columns} is {@code null}.
   * @throws RuntimeException if a partition column does not exist in this DataFrame's schema.
   */
  public DataFrame repartitionHash(int numPartitions, String... columns) {
    long h = handle.acquire();
    try {
      if (numPartitions <= 0) {
        throw new IllegalArgumentException("numPartitions must be positive, was " + numPartitions);
      }
      if (columns == null) {
        throw new IllegalArgumentException("repartitionHash columns must be non-null");
      }
      if (columns.length == 0) {
        throw new IllegalArgumentException("repartitionHash requires at least one column");
      }
      for (int i = 0; i < columns.length; i++) {
        if (columns[i] == null) {
          throw new IllegalArgumentException("repartitionHash columns[" + i + "] must be non-null");
        }
      }
      return new DataFrame(repartitionHashRows(h, numPartitions, columns));
    } finally {
      handle.release();
    }
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
    long l = handle.acquire();
    try {
      long r = right.acquireAsOperand("right DataFrame is closed or already collected");
      try {
        return new DataFrame(joinDataFrame(l, r, type.code(), leftCols, rightCols, null));
      } finally {
        right.handle.release();
      }
    } finally {
      handle.release();
    }
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
    long l = handle.acquire();
    try {
      long r = right.acquireAsOperand("right DataFrame is closed or already collected");
      try {
        if (filter == null) {
          throw new IllegalArgumentException("join filter must be non-null");
        }
        return new DataFrame(joinDataFrame(l, r, type.code(), leftCols, rightCols, filter));
      } finally {
        right.handle.release();
      }
    } finally {
      handle.release();
    }
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
    long l = handle.acquire();
    try {
      long r = right.acquireAsOperand("right DataFrame is closed or already collected");
      try {
        return new DataFrame(joinOnDataFrame(l, r, type.code(), predicates));
      } finally {
        right.handle.release();
      }
    } finally {
      handle.release();
    }
  }

  private static void checkJoinArgs(
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
    long h = handle.acquire();
    try {
      writeParquetWithOptions(
          h,
          path,
          options.compression(),
          options.singleFileOutput() != null,
          options.singleFileOutput() != null && options.singleFileOutput());
    } finally {
      handle.release();
    }
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
    long h = handle.acquire();
    try {
      if (path == null) {
        throw new IllegalArgumentException("writeCsv path must be non-null");
      }
      if (options == null) {
        throw new IllegalArgumentException("writeCsv options must be non-null");
      }
      writeCsvWithOptions(h, path, options.toBytes());
    } finally {
      handle.release();
    }
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
    long h = handle.acquire();
    try {
      if (path == null) {
        throw new IllegalArgumentException("writeJson path must be non-null");
      }
      if (options == null) {
        throw new IllegalArgumentException("writeJson options must be non-null");
      }
      writeJsonWithOptions(h, path, options.toBytes());
    } finally {
      handle.release();
    }
  }

  /**
   * Release the native plan. Blocks until calls already in flight on this DataFrame return, so that
   * no native call can be left dereferencing a freed plan. Idempotent, and a no-op if the DataFrame
   * was already consumed by {@link #collect} or {@link #executeStream}.
   */
  @Override
  public void close() {
    long h = handle.claimQuietly();
    if (h != 0) {
      closeDataFrame(h);
    }
  }

  private static native void collectDataFrame(long handle, long ffiStreamAddr);

  private static native void executeStreamDataFrame(long handle, long ffiStreamAddr);

  private static native void closeDataFrame(long handle);

  private static native long countRows(long handle);

  private static native byte[] schemaIpc(long handle);

  private static native long explainPlan(long handle, boolean verbose, boolean analyze);

  private static native long cachePlan(long handle);

  private static native long describePlan(long handle);

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

  private static native long unionRows(long handle, long otherHandle);

  private static native long unionDistinctRows(long handle, long otherHandle);

  private static native long unionByNameRows(long handle, long otherHandle);

  private static native long unionByNameDistinctRows(long handle, long otherHandle);

  private static native long intersectRows(long handle, long otherHandle);

  private static native long intersectDistinctRows(long handle, long otherHandle);

  private static native long exceptRows(long handle, long otherHandle);

  private static native long exceptDistinctRows(long handle, long otherHandle);

  private static native long sortRows(
      long handle, String[] columns, boolean[] ascending, boolean[] nullsFirst);

  private static native long repartitionRoundRobinRows(long handle, int numPartitions);

  private static native long repartitionHashRows(long handle, int numPartitions, String[] columns);

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
