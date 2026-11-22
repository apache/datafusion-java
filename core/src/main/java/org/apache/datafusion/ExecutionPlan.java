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

/**
 * A bounded Java-backed leaf plan returned by {@link TableProvider#scanWithArgs(ScanArgs)}.
 *
 * <p>Constructing the scan must not read source rows. Each execution opens a fresh independent
 * reader whose schema is either the registered full schema or its requested projection. Reader
 * buffers must use the supplied allocator or a child of it. DataFusion owns and closes the reader.
 *
 * <p>This is a Java-backed scan, not an API for arbitrary child-consuming physical operators.
 */
@FunctionalInterface
public interface ExecutionPlan {
  /**
   * Open an independent stream for a partition during physical execution.
   *
   * <p>The current adapter executes only partition {@code 0}; the partition argument is explicit so
   * supporting multi-partition plans later does not require changing this method's signature.
   */
  ArrowReader execute(int partition, BufferAllocator allocator);
}
