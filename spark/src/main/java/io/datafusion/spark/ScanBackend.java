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
 * Native scan surface the connector plumbing talks to. One method per JNI entry point of the
 * {@code datafusion-spark-bridge} scan machinery; implementations only differ in <em>which</em>
 * native library and class the calls land on:
 *
 * <ul>
 *   <li>{@link FfiScanBackend} (the {@link FfiProviderFactory#scanBackend()} default) builds the
 *       provider via {@link FfiProviderFactory#createProvider(byte[], byte[])} and routes through
 *       the connector's own cdylib ({@link FfiHelperNative}) — the generic FFI path.
 *   <li>A static bridge supplies its own implementation delegating to the class it named in its
 *       {@code export_bridge!} invocation, whose generated {@code createScan} builds the provider
 *       from {@code options}/{@code partitionBytes} directly — no pointer handover, no
 *       {@code datafusion-ffi}.
 * </ul>
 *
 * <p>Implementations must be stateless or thread-safe: the driver probes schemas and plans through
 * one instance while executor tasks stream through others, and scan handles are shared across
 * threads by the shared-scan cache. Handle-based methods accept handles produced by {@code
 * createScan} on any instance of the same implementation.
 */
public interface ScanBackend {

  /**
   * Driver-side schema probe: the widened Arrow schema of the provider described by {@code
   * options} + {@code partitionBytes}, serialized as Arrow IPC bytes (deserialize with {@code
   * MessageSerializer.deserializeSchema}).
   */
  byte[] providerSchemaIpc(byte[] options, byte[] partitionBytes);

  /**
   * Build a planned scan and return its handle. {@code targetPartitions}/{@code batchSize} {@code
   * <= 0} leave DataFusion defaults; {@code optionKeys}/{@code optionValues} are parallel config
   * override arrays; empty {@code projectionColumns} selects all columns; each {@code
   * filterProtos} element is a serialized {@code datafusion.LogicalExprNode}.
   *
   * <p>The caller owns the handle and must pair it with {@link #closeScan(long)}. Closing while a
   * stream opened from the handle is in flight is undefined behaviour — the shared-scan cache's
   * refcount enforces this; any other caller must serialize close itself.
   */
  long createScan(
      byte[] options,
      byte[] partitionBytes,
      int targetPartitions,
      int batchSize,
      String[] optionKeys,
      String[] optionValues,
      String[] projectionColumns,
      byte[][] filterProtos);

  /** Output partition count of the planned physical plan. */
  int partitionCount(long scanHandle);

  /**
   * Open an independent stream over ONE plan partition, writing an {@code FFI_ArrowArrayStream}
   * into the caller-allocated struct at {@code ffiStreamAddr}. Concurrent-safe across JVM threads.
   */
  void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr);

  /**
   * Stream the WHOLE plan (all partitions coalesced) into the caller-allocated {@code
   * FFI_ArrowArrayStream} at {@code ffiStreamAddr}. Used by legacy per-partition payload mode.
   */
  void executeStream(long scanHandle, long ffiStreamAddr);

  /** Drop the planned scan. See {@link #createScan} for the close-vs-in-flight contract. */
  void closeScan(long scanHandle);
}
