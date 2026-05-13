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
 * A lazy representation of a query plan, mirroring the Rust DataFusion {@code DataFrame}. Created
 * by {@link SessionContext#sql(String)} or other planning entry points and executed by {@link
 * #collect}.
 *
 * <p>Instances are <strong>not thread-safe</strong> and must be closed. {@link #collect} consumes
 * the DataFrame: a successfully collected DataFrame cannot be collected again, and {@link #close()}
 * on an already-collected instance is a no-op.
 */
public final class DataFrame implements AutoCloseable {
  static {
    NativeLibraryLoader.loadLibrary();
  }

  private long nativeHandle;

  DataFrame(long nativeHandle) {
    if (nativeHandle == 0) {
      throw new IllegalArgumentException("DataFrame native handle is null");
    }
    this.nativeHandle = nativeHandle;
  }

  /**
   * Execute the plan and return its record batches as an {@link ArrowReader}.
   *
   * <p>Consumes this DataFrame: the native plan is released as soon as the stream is established.
   * The caller is responsible for closing the returned reader, and the supplied allocator must
   * outlive it.
   */
  public ArrowReader collect(BufferAllocator allocator) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    long handle = nativeHandle;
    nativeHandle = 0;
    try {
      collectDataFrame(handle, stream.memoryAddress());
      return Data.importArrayStream(allocator, stream);
    } catch (Throwable e) {
      stream.close();
      throw e;
    }
  }

  /** Execute the plan and return the number of rows. */
  public long count() {
    if (nativeHandle == 0) {
      throw new IllegalStateException("DataFrame is closed or already collected");
    }
    return countRows(nativeHandle);
  }

  @Override
  public void close() {
    if (nativeHandle != 0) {
      closeDataFrame(nativeHandle);
      nativeHandle = 0;
    }
  }

  private static native void collectDataFrame(long handle, long ffiStreamAddr);

  private static native void closeDataFrame(long handle);

  private static native long countRows(long handle);
}
