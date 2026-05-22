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

import java.util.function.Function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A {@link TableProvider} that pairs a fixed {@link Schema} with a function that opens a fresh
 * {@link ArrowReader} for each scan. Provided as a convenience for the common case where there is
 * no projection / filter pushdown to implement.
 *
 * <p>Each call to {@link #scan(BufferAllocator)} invokes the supplied function and returns whatever
 * {@link ArrowReader} it produces, so the function MUST return a fresh, independent reader on every
 * invocation (see the contract on {@link TableProvider#scan(BufferAllocator)}).
 *
 * <p>As {@link TableProvider} grows additional methods in the future, this class will provide
 * defaults so existing callers keep working without changes.
 */
public final class SimpleTableProvider implements TableProvider {

  private final Schema schema;
  private final Function<BufferAllocator, ArrowReader> scanFn;

  /**
   * @param schema the table schema; returned as-is from {@link #schema()}
   * @param scanFn called on every {@link #scan(BufferAllocator)} with the framework-supplied
   *     allocator; must return a fresh, independent {@link ArrowReader} each time
   */
  public SimpleTableProvider(Schema schema, Function<BufferAllocator, ArrowReader> scanFn) {
    if (schema == null) {
      throw new IllegalArgumentException("schema must be non-null");
    }
    if (scanFn == null) {
      throw new IllegalArgumentException("scanFn must be non-null");
    }
    this.schema = schema;
    this.scanFn = scanFn;
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public ArrowReader scan(BufferAllocator allocator) {
    return scanFn.apply(allocator);
  }
}
