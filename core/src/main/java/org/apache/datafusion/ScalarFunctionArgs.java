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
import java.util.Objects;

/**
 * Bundle of inputs passed to {@link ScalarFunction#evaluate}: the per-arg {@link ColumnarValue}s
 * (in declared order) and the batch row count DataFusion is driving.
 *
 * <p>Mirrors DataFusion's {@code datafusion::logical_expr::ScalarFunctionArgs}. {@code rowCount} is
 * the only channel by which an array-returning UDF without array-typed inputs (all-scalar args, or
 * nullary) can size its output. Nullary UDFs that prefer to broadcast a single value should return
 * {@link ColumnarValue#scalar(org.apache.arrow.vector.FieldVector) ColumnarValue.scalar(...)}
 * instead, which removes the need to consult {@code rowCount}.
 */
public record ScalarFunctionArgs(List<ColumnarValue> args, int rowCount) {
  public ScalarFunctionArgs {
    args = List.copyOf(Objects.requireNonNull(args, "args"));
    if (rowCount < 0) {
      throw new IllegalArgumentException("rowCount must be >= 0, got " + rowCount);
    }
  }
}
