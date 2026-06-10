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

package io.datafusion.spark;

/**
 * JNI hooks into the connector-core widening cdylib ({@code
 * libdatafusion_spark_helper.{so,dylib}}).
 *
 * <p>The widening cdylib unwraps an FFI_TableProvider pointer produced by a bridge, wraps it in a
 * {@code WideningTableProvider} that applies kernel-level {@code arrow::compute::cast} on incoming
 * RecordBatches for any Spark-incompatible Arrow type (unsigned ints, Float16, Time,
 * non-microsecond Timestamp, recursive List), and re-FFIs it for the consumer (datafusion-java's
 * cdylib via {@code SessionContext.registerFfiTable}).
 *
 * <p>The native library is loaded once per JVM via {@link NativeLibraryLoader}. The library payload
 * lives inside this jar under {@code io/datafusion/spark/<os>/<arch>/} and is extracted to a temp
 * file before {@link System#load}.
 */
public final class FfiHelperNative {

  private FfiHelperNative() {}

  static {
    NativeLibraryLoader.loadLibrary("datafusion_spark_helper");
  }

  /**
   * Take ownership of an {@code FFI_TableProvider} pointer produced by a bridge cdylib, wrap it in
   * a {@code WideningTableProvider}, and re-wrap the result as a fresh {@code FFI_TableProvider}.
   * Returns the new raw pointer; the caller owns it.
   *
   * <p>The input pointer must not be reused after this call returns: ownership transfers.
   */
  public static native long wrapWithWidening(long ffiProviderRawPtr);
}
