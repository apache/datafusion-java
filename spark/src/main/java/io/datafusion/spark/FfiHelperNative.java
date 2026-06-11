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
 * JNI surface of the connector cdylib ({@code libdatafusion_spark_helper.{so,dylib}}).
 *
 * <p>The cdylib owns the whole DataFusion side of a scan: it takes an {@code FFI_TableProvider}
 * pointer produced by a bridge, wraps the provider in a {@code WideningTableProvider} (kernel-level
 * {@code arrow::compute::cast} for Spark-incompatible Arrow types), registers it on a private
 * {@code SessionContext} built from the driver-pinned config, applies the pruned projection and the
 * proto-encoded pushed filters, plans once, and streams plan partitions back over {@code
 * FFI_ArrowArrayStream}.
 *
 * <p>Errors throw the typed {@code org.apache.datafusion.*} exception hierarchy (from the
 * datafusion-java core jar, a compile dependency of this module).
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
   * Driver-side schema probe: the widened Arrow schema of the provider, serialized as Arrow IPC
   * bytes (deserialize with {@code MessageSerializer.deserializeSchema}).
   *
   * <p>Takes ownership of {@code ffiProviderRawPtr}; the provider is dropped before returning and
   * the pointer must not be reused.
   */
  public static native byte[] providerSchemaIpc(long ffiProviderRawPtr);

  /**
   * Build a planned scan over the provider and return its handle.
   *
   * <p>Takes ownership of {@code ffiProviderRawPtr}. {@code targetPartitions} / {@code batchSize}
   * {@code <= 0} leave the DataFusion defaults; {@code optionKeys}/{@code optionValues} are
   * parallel arrays of DataFusion config overrides; an empty {@code projectionColumns} selects all
   * columns; each element of {@code filterProtos} is a serialized {@code datafusion.LogicalExprNode}
   * applied as a filter.
   *
   * <p>The caller owns the returned handle and must pair it with {@link #closeScan(long)}. Closing
   * while a stream opened from this handle is still in flight is undefined behaviour — the
   * shared-scan cache's refcount enforces this; any other caller must serialize close itself.
   */
  public static native long createScan(
      long ffiProviderRawPtr,
      int targetPartitions,
      int batchSize,
      String[] optionKeys,
      String[] optionValues,
      String[] projectionColumns,
      byte[][] filterProtos);

  /** Output partition count of the planned physical plan. */
  public static native int partitionCount(long scanHandle);

  /**
   * Open an independent stream over ONE plan partition, writing an {@code FFI_ArrowArrayStream}
   * into the caller-allocated struct at {@code ffiStreamAddr}. Concurrent-safe across JVM threads.
   */
  public static native void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr);

  /**
   * Stream the WHOLE plan (all partitions coalesced) into the caller-allocated {@code
   * FFI_ArrowArrayStream} at {@code ffiStreamAddr}. Used by legacy per-partition payload mode,
   * where the provider itself already represents the task's slice.
   */
  public static native void executeStream(long scanHandle, long ffiStreamAddr);

  /** Drop the planned scan. See {@link #createScan} for the close-vs-in-flight-stream contract. */
  public static native void closeScan(long scanHandle);
}
