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

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * A DataFrame planned exactly once, exposing its physical plan's output partitions for individual
 * streaming. Obtained via {@link DataFrame#toPartitionedExecution()}.
 *
 * <p>Unlike {@link DataFrame#executeStream(BufferAllocator)}, which coalesces every output
 * partition into one stream, this handle lets distinct threads drive distinct partitions — e.g. one
 * Spark task per DataFusion partition.
 *
 * <p><b>Thread safety.</b> {@link #partitionCount()} and {@link #executeStream(int,
 * BufferAllocator)} are safe to call concurrently from multiple threads on the same instance.
 * Re-executing the same partition index more than once opens an independent native stream each
 * time, but only succeeds when every operator in that partition's pipeline supports repeated {@code
 * execute()} — stateless scans (MemTable, table providers) do; {@code RepartitionExec} pipelines
 * (hash aggregates, joins) do not and fail the second stream. {@link #close()} is idempotent, but
 * the caller must guarantee that no {@code executeStream} call is in flight and that all returned
 * readers have been closed before calling it — the native plan is freed immediately. Consumers that
 * share one instance across threads must enforce that ordering themselves (e.g. with a reference
 * count).
 */
public final class PartitionedExecution implements AutoCloseable {
  static {
    NativeLibraryLoader.loadLibrary();
  }

  private volatile long nativeHandle;

  PartitionedExecution(long nativeHandle) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("Failed to create native PartitionedExecution");
    }
    this.nativeHandle = nativeHandle;
  }

  /** Number of output partitions of the planned physical plan. */
  public int partitionCount() {
    long handle = nativeHandle;
    if (handle == 0) {
      throw new IllegalStateException("PartitionedExecution is closed");
    }
    return partitionCountNative(handle);
  }

  /**
   * Open an independent stream over one plan partition. Each call to {@link
   * ArrowReader#loadNextBatch} drives one async {@code stream.next()} on the native side, so memory
   * pressure stays bounded by the executor pipeline plus one in-flight batch.
   *
   * <p>Non-consuming: this instance remains usable, and concurrent calls — including for the same
   * partition index — are safe. The caller closes the returned reader; the supplied allocator must
   * outlive it.
   *
   * @param partition partition index in {@code [0, partitionCount())}
   * @throws RuntimeException if the index is out of range for the planned partitioning
   */
  public ArrowReader executeStream(int partition, BufferAllocator allocator) {
    long handle = nativeHandle;
    if (handle == 0) {
      throw new IllegalStateException("PartitionedExecution is closed");
    }
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    try {
      executeStreamPartition(handle, partition, stream.memoryAddress());
      return Data.importArrayStream(allocator, stream);
    } catch (Throwable e) {
      stream.close();
      throw e;
    }
  }

  /**
   * Release the native plan. Idempotent. See the class Javadoc for the ordering contract with
   * in-flight {@link #executeStream(int, BufferAllocator)} calls.
   */
  @Override
  public void close() {
    long handle = nativeHandle;
    if (handle != 0) {
      nativeHandle = 0;
      closePartitionedExecution(handle);
    }
  }

  private static native int partitionCountNative(long handle);

  private static native void executeStreamPartition(long handle, int partition, long ffiStreamAddr);

  private static native void closePartitionedExecution(long handle);
}
