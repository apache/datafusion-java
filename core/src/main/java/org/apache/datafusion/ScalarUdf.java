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

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

/**
 * A Java-implemented scalar SQL function. Register an instance with
 * {@link SessionContext#registerUdf} to make it callable from SQL or DataFrame plans.
 *
 * <p>Implementations may be invoked concurrently by DataFusion on multiple worker threads.
 * If the implementation carries mutable state, the implementation must synchronize it.
 */
@FunctionalInterface
public interface ScalarUdf {
  /**
   * Compute the function result for one input batch.
   *
   * @param allocator the {@link BufferAllocator} that MUST be used for any new
   *     {@link FieldVector} allocation, including the result. Buffers allocated from
   *     other allocators will not survive the JNI handoff.
   * @param args one {@link FieldVector} per declared argument, all of the same length.
   *     These are read-only views; the implementation must NOT close them.
   * @return a {@link FieldVector} of the declared return type and the same length as
   *     the inputs. Ownership transfers to the framework on return; the implementation
   *     must NOT close the returned vector.
   */
  FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args);
}
