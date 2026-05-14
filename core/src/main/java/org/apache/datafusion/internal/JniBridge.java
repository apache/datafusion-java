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

package org.apache.datafusion.internal;

import java.util.List;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.datafusion.ScalarUdf;

/** Internal trampoline invoked from native code on every UDF call. Not part of the public API. */
public final class JniBridge {
  /**
   * Shared allocator for UDF inputs/outputs. Created once at class-load time; never closed.
   * Outstanding allocations are released by the FFI structs' release callbacks when the native side
   * drops them after {@code from_ffi}.
   */
  private static final RootAllocator ALLOCATOR = new RootAllocator();

  private JniBridge() {}

  /**
   * Invoke a scalar UDF for one batch. Called from native code; not for application use.
   *
   * @param udf the registered {@link ScalarUdf} instance
   * @param argsArrayAddr address of a populated {@code FFI_ArrowArray} struct holding the input
   *     batch as a struct array (one field per UDF argument)
   * @param argsSchemaAddr address of the matching {@code FFI_ArrowSchema}
   * @param resultArrayAddr address of an empty {@code FFI_ArrowArray} the bridge writes into
   * @param resultSchemaAddr address of an empty {@code FFI_ArrowSchema} the bridge writes into
   * @param expectedRowCount the row count the result vector must have
   */
  public static void invokeScalarUdf(
      ScalarUdf udf,
      long argsArrayAddr,
      long argsSchemaAddr,
      long resultArrayAddr,
      long resultSchemaAddr,
      int expectedRowCount) {
    ArrowArray argsArr = ArrowArray.wrap(argsArrayAddr);
    ArrowSchema argsSch = ArrowSchema.wrap(argsSchemaAddr);
    ArrowArray resultArr = ArrowArray.wrap(resultArrayAddr);
    ArrowSchema resultSch = ArrowSchema.wrap(resultSchemaAddr);

    try (VectorSchemaRoot root = Data.importVectorSchemaRoot(ALLOCATOR, argsArr, argsSch, null)) {
      List<FieldVector> argVectors = root.getFieldVectors();

      FieldVector result = udf.evaluate(ALLOCATOR, argVectors);

      if (result == null) {
        throw new IllegalStateException("ScalarUdf.evaluate returned null");
      }
      if (result.getValueCount() != expectedRowCount) {
        try {
          throw new IllegalStateException(
              "ScalarUdf.evaluate returned vector with "
                  + result.getValueCount()
                  + " rows; expected "
                  + expectedRowCount);
        } finally {
          result.close();
        }
      }

      try {
        Data.exportVector(ALLOCATOR, result, null, resultArr, resultSch);
      } finally {
        result.close();
      }
    }
  }
}
