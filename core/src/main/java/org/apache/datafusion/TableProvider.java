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

import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.protobuf.LogicalExprNode;

/**
 * A Java-implemented table that can be registered with a {@link SessionContext} via {@link
 * SessionContext#registerTable(String, TableProvider)}. Mirrors the role of DataFusion's Rust
 * {@code TableProvider} trait. Implement {@link #scan(BufferAllocator)} for full-schema reads, or
 * override {@link #scanWithArgs(ScanArgs)} to plan a lazy scan with projection/filter/limit
 * pushdown.
 *
 * <p>{@link SimpleTableProvider} is a ready-made implementation for the common case of "I have a
 * schema and a function that returns an {@link ArrowReader}".
 *
 * <p>Each execution must return a fresh, independent {@link ArrowReader} so that queries which
 * touch the table more than once (self-joins, {@code UNION ALL}, repeated reads) work correctly.
 * The returned reader is closed by the framework when the stream ends.
 *
 * <p>The schema returned by {@link #schema()} is captured once at registration time. Every batch
 * produced by {@link #scan(BufferAllocator)} must conform to it. A plan returned by {@link
 * #scanWithArgs(ScanArgs)} may instead produce the requested projected schema.
 */
public interface TableProvider {
  /** DataFusion's per-filter pushdown contract. */
  enum FilterPushdown {
    /** The source does not apply this filter; DataFusion keeps it above the scan. */
    UNSUPPORTED,
    /** The source prunes candidates; DataFusion also applies the filter to the returned rows. */
    INEXACT,
    /** The source completely applies the filter with DataFusion-compatible semantics. */
    EXACT
  }

  /** The fixed schema of this table. Called once, at registration time. */
  Schema schema();

  /**
   * Open a fresh batch stream for this table. Called once per physical scan of the table — a single
   * query may invoke this more than once (self-joins, {@code UNION ALL} over the same table, etc.).
   *
   * <p>Each invocation MUST return an independent {@link ArrowReader}. The reader's schema MUST
   * equal {@link #schema()}. The reader's buffers MUST be allocated from {@code allocator} (or from
   * a child of it) — the framework needs the reader's allocator hierarchy to share a root with the
   * one it passes here. The allocator contract mirrors the one on {@link ScalarFunction#evaluate}.
   *
   * <p>Providers that implement only {@link #scanWithArgs(ScanArgs)} may leave this method
   * unsupported.
   */
  default ArrowReader scan(BufferAllocator allocator) {
    throw new UnsupportedOperationException("Implement scan or scanWithArgs");
  }

  /**
   * Plan one bounded scan without opening its reader. The default defers to {@link
   * #scan(BufferAllocator)} at execution time, preserving existing providers.
   *
   * <p>Only supported/inexact filters are included in {@code args}. The plan may produce either the
   * registered full schema or exactly the requested projection, including zero-column batches that
   * preserve row counts. The native adapter projects full-schema batches when needed.
   *
   * <p>Planning may be repeated and must not read source rows. Callbacks may run on native worker
   * threads; shared provider state and reader factories must be thread-safe.
   */
  default ExecutionPlan scanWithArgs(ScanArgs args) {
    return (partition, allocator) -> scan(allocator);
  }

  /**
   * Return one capability per input filter, in the same order. The default declines every filter.
   * An inexact filter must never discard a matching row. Expressions are generated protobuf
   * messages; source-specific access-path decisions remain in the provider.
   */
  default List<FilterPushdown> supportsFiltersPushdown(List<LogicalExprNode> filters) {
    return Collections.nCopies(filters.size(), FilterPushdown.UNSUPPORTED);
  }
}
