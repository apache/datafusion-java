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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A Java-implemented table that can be registered with a {@link SessionContext} via {@link
 * SessionContext#registerTable(String, TableProvider)}. Mirrors the role of DataFusion's Rust
 * {@code TableProvider} trait, but at present only exposes the methods needed for a full table
 * scan; future versions may add filter/projection pushdown and multi-partition support as default
 * methods so existing implementations keep working.
 *
 * <p>{@link SimpleTableProvider} is a ready-made implementation for the common case of "I have a
 * schema and a function that returns an {@link ArrowReader}".
 *
 * <p>Each call to {@link #scan(BufferAllocator)} must return a fresh, independent {@link
 * ArrowReader} so that queries which touch the table more than once (self-joins, {@code UNION ALL},
 * repeated reads) work correctly. The returned reader is closed by the framework when the stream
 * ends.
 *
 * <p>The schema returned by {@link #schema()} is captured once at registration time. Every batch
 * produced by every {@code ArrowReader} returned from {@link #scan(BufferAllocator)} must conform
 * to it; a mismatch fails the query.
 */
public interface TableProvider {
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
   */
  ArrowReader scan(BufferAllocator allocator);
}
