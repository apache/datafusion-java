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
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * A Java-implemented scalar SQL function. Implementations declare their own name, signature, and
 * volatility, and evaluate one input batch at a time.
 *
 * <p>Mirrors DataFusion's {@code ScalarUDFImpl} trait. Wrap an instance in a {@link ScalarUdf} and
 * register it via {@link SessionContext#registerUdf(ScalarUdf)} to make it callable from SQL or
 * DataFrame plans.
 *
 * <p>Implementations may be invoked concurrently by DataFusion on multiple worker threads. If the
 * implementation carries mutable state, the implementation must synchronize it.
 */
public interface ScalarFunction {
  /** SQL name under which this function is invoked (e.g., {@code "add_one"}). */
  String name();

  /**
   * Declared argument types, in positional order. The function is registered with an exact
   * signature; calls whose argument types do not match exactly are rejected.
   */
  List<ArrowType> argTypes();

  /** Declared return type. The returned {@link FieldVector} must have this exact type. */
  ArrowType returnType();

  /**
   * Volatility classification. Use {@link Volatility#IMMUTABLE} for pure functions, {@link
   * Volatility#STABLE} for functions deterministic within a query, and {@link Volatility#VOLATILE}
   * for non-deterministic functions.
   */
  Volatility volatility();

  /**
   * Compute the function result for one input batch.
   *
   * @param allocator the {@link BufferAllocator} that MUST be used for any new {@link FieldVector}
   *     allocation, including the result. Buffers allocated from other allocators will not survive
   *     the JNI handoff.
   * @param args one {@link FieldVector} per declared argument, all of the same length. These are
   *     read-only views; the implementation must NOT close them.
   * @return a {@link FieldVector} of the declared return type and the same length as the inputs.
   *     Ownership transfers to the framework on return; the implementation must NOT close the
   *     returned vector.
   */
  FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args);
}
