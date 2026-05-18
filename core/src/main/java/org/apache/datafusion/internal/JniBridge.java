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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.ColumnarValue;
import org.apache.datafusion.DataSource;
import org.apache.datafusion.ScalarFunction;
import org.apache.datafusion.ScalarFunctionArgs;

/** Internal trampoline invoked from native code on every UDF call. Not part of the public API. */
public final class JniBridge {
  /**
   * Shared allocator for UDF inputs/outputs. Created once at class-load time; never closed.
   * Outstanding allocations are released by the FFI structs' release callbacks when the native side
   * drops them after {@code from_ffi}.
   */
  private static final RootAllocator ALLOCATOR = new RootAllocator();

  private JniBridge() {}

  /** argKind byte signalling a {@link ColumnarValue.Array} arg. */
  private static final byte KIND_ARRAY = 0;

  /** argKind byte signalling a {@link ColumnarValue.Scalar} arg. */
  private static final byte KIND_SCALAR = 1;

  /**
   * Invoke a scalar UDF for one batch. Called from native code; not for application use.
   *
   * <p>Args arrive split into two struct arrays: {@code arrayArgs*} of length {@code rowCount}
   * holding the {@link ColumnarValue.Array} arguments in their relative order, and {@code
   * scalarArgs*} of length 1 holding the {@link ColumnarValue.Scalar} arguments. {@code argKinds}
   * records the original positional order so the bridge can interleave them back into a single
   * {@code List<ColumnarValue>} for the user.
   *
   * @return {@link #KIND_ARRAY} if the UDF returned an Array, {@link #KIND_SCALAR} if it returned a
   *     Scalar. The native caller uses this to reconstruct the right {@code ColumnarValue} variant.
   */
  public static byte invokeScalarUdf(
      ScalarFunction impl,
      long arrayArgsArrayAddr,
      long arrayArgsSchemaAddr,
      long scalarArgsArrayAddr,
      long scalarArgsSchemaAddr,
      byte[] argKinds,
      long resultArrayAddr,
      long resultSchemaAddr,
      int rowCount) {
    ArrowArray arrayArr = ArrowArray.wrap(arrayArgsArrayAddr);
    ArrowSchema arraySch = ArrowSchema.wrap(arrayArgsSchemaAddr);
    ArrowArray scalarArr = ArrowArray.wrap(scalarArgsArrayAddr);
    ArrowSchema scalarSch = ArrowSchema.wrap(scalarArgsSchemaAddr);
    ArrowArray resultArr = ArrowArray.wrap(resultArrayAddr);
    ArrowSchema resultSch = ArrowSchema.wrap(resultSchemaAddr);

    try (VectorSchemaRoot arrayRoot =
            Data.importVectorSchemaRoot(ALLOCATOR, arrayArr, arraySch, null);
        VectorSchemaRoot scalarRoot =
            Data.importVectorSchemaRoot(ALLOCATOR, scalarArr, scalarSch, null)) {

      List<FieldVector> arrayFields = arrayRoot.getFieldVectors();
      List<FieldVector> scalarFields = scalarRoot.getFieldVectors();

      List<ColumnarValue> args = new ArrayList<>(argKinds.length);
      int arrayIdx = 0;
      int scalarIdx = 0;
      for (byte kind : argKinds) {
        if (kind == KIND_ARRAY) {
          args.add(ColumnarValue.array(arrayFields.get(arrayIdx++)));
        } else if (kind == KIND_SCALAR) {
          args.add(ColumnarValue.scalar(scalarFields.get(scalarIdx++)));
        } else {
          throw new IllegalStateException("Unknown argKind byte: " + kind);
        }
      }

      ColumnarValue result = impl.evaluate(ALLOCATOR, new ScalarFunctionArgs(args, rowCount));

      if (result == null) {
        throw new IllegalStateException("ScalarFunction.evaluate returned null");
      }

      FieldVector resultVec = result.vector();
      byte resultKind;
      int expectedLen;
      if (result instanceof ColumnarValue.Array) {
        resultKind = KIND_ARRAY;
        expectedLen = rowCount;
      } else {
        resultKind = KIND_SCALAR;
        expectedLen = 1;
      }

      if (resultVec.getValueCount() != expectedLen) {
        try {
          throw new IllegalStateException(
              "ScalarFunction.evaluate returned "
                  + (resultKind == KIND_ARRAY ? "Array" : "Scalar")
                  + " vector with "
                  + resultVec.getValueCount()
                  + " rows; expected "
                  + expectedLen);
        } finally {
          resultVec.close();
        }
      }

      try {
        Data.exportVector(ALLOCATOR, resultVec, null, resultArr, resultSch);
      } finally {
        resultVec.close();
      }

      return resultKind;
    }
  }

  /**
   * Open a fresh batch stream from a Java {@link DataSource} and export it through the supplied
   * Arrow C Data Interface address. Called from native code; not for application use.
   *
   * <p>On success, ownership of the returned reader transfers to the FFI stream's release callback,
   * so the native side closing the stream also closes the reader. On any failure during export, the
   * reader is closed here before the exception propagates.
   */
  public static void invokeDataSourceScan(DataSource source, long ffiStreamAddr) {
    ArrowReader reader = source.scan();
    if (reader == null) {
      throw new IllegalStateException("DataSource.scan returned null");
    }
    ArrowArrayStream stream = ArrowArrayStream.wrap(ffiStreamAddr);
    try {
      Data.exportArrayStream(ALLOCATOR, reader, stream);
    } catch (Throwable t) {
      try {
        reader.close();
      } catch (Exception ignored) {
        // best-effort cleanup; original error wins
      }
      throw t;
    }
  }
}
