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

import java.util.Objects;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * The value of a scalar UDF argument or result: either a per-row {@link Array} of length {@code
 * rowCount}, or a {@link Scalar} (length-1 vector) that the framework broadcasts.
 *
 * <p>Mirrors DataFusion's {@code datafusion::logical_expr::ColumnarValue} enum. Use {@link
 * #array(FieldVector)} and {@link #scalar(FieldVector)} factories rather than constructing the
 * records directly so the length invariants are enforced consistently.
 */
public sealed interface ColumnarValue permits ColumnarValue.Array, ColumnarValue.Scalar {

  /** The underlying Arrow vector. For {@link Scalar} this vector has {@code valueCount == 1}. */
  FieldVector vector();

  /** Convenience: the vector's declared Arrow type. */
  default ArrowType dataType() {
    return vector().getField().getType();
  }

  /** Wrap an arbitrary-length vector as an {@link Array}. */
  static ColumnarValue array(FieldVector vector) {
    return new Array(Objects.requireNonNull(vector, "vector"));
  }

  /**
   * Wrap a length-1 vector as a {@link Scalar}.
   *
   * @throws IllegalArgumentException if {@code vector.getValueCount() != 1}
   */
  static ColumnarValue scalar(FieldVector vector) {
    Objects.requireNonNull(vector, "vector");
    if (vector.getValueCount() != 1) {
      throw new IllegalArgumentException(
          "Scalar vector must have valueCount == 1, got " + vector.getValueCount());
    }
    return new Scalar(vector);
  }

  /** Per-row Arrow vector of length equal to the batch row count. */
  record Array(FieldVector vector) implements ColumnarValue {
    public Array {
      Objects.requireNonNull(vector, "vector");
    }
  }

  /** Length-1 Arrow vector representing a single value broadcast across all rows. */
  record Scalar(FieldVector vector) implements ColumnarValue {
    public Scalar {
      Objects.requireNonNull(vector, "vector");
      if (vector.getValueCount() != 1) {
        throw new IllegalArgumentException(
            "Scalar vector must have valueCount == 1, got " + vector.getValueCount());
      }
    }
  }
}
