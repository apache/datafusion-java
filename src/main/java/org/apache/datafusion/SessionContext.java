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

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A DataFusion session context.
 *
 * <p>Instances are <strong>not thread-safe</strong>. Concurrent calls to any of {@link #sql},
 * {@link #registerParquet}, or {@link #close} from different threads can produce a use-after-free
 * on the native side. Callers must externally synchronize, or confine each context to a single
 * thread.
 */
public final class SessionContext implements AutoCloseable {
  static {
    NativeLibraryLoader.loadLibrary();
  }

  private long nativeHandle;

  public SessionContext() {
    this.nativeHandle = createSessionContext();
    if (this.nativeHandle == 0) {
      throw new RuntimeException("Failed to create native SessionContext");
    }
  }

  /**
   * Parse and plan {@code query}, returning a lazy {@link DataFrame}. The query is not executed
   * until {@link DataFrame#collect} is called.
   */
  public DataFrame sql(String query) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    long dfHandle = createDataFrame(nativeHandle, query);
    return new DataFrame(dfHandle);
  }

  /**
   * Decode a DataFusion-Proto {@code LogicalPlanNode} and return a lazy {@link DataFrame}. The plan
   * is not executed until {@link DataFrame#collect} is called.
   *
   * <p>The bytes must be a serialized {@code datafusion.LogicalPlanNode} (see {@code
   * org.apache.datafusion.protobuf.LogicalPlanNode}).
   *
   * @throws RuntimeException if the bytes are not a valid {@code LogicalPlanNode} or if logical
   *     planning fails.
   */
  public DataFrame fromProto(byte[] planBytes) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    long dfHandle = createDataFrameFromProto(nativeHandle, planBytes);
    return new DataFrame(dfHandle);
  }

  /**
   * Return the Arrow {@link Schema} of a registered table. Transferred via Arrow IPC; no {@link
   * org.apache.arrow.memory.BufferAllocator} is required because a schema carries no buffer data.
   *
   * @throws RuntimeException if {@code tableName} is not registered in this context.
   */
  public Schema tableSchema(String tableName) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    byte[] ipcBytes = tableSchemaIpc(nativeHandle, tableName);
    try {
      return MessageSerializer.deserializeSchema(
          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(ipcBytes))));
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to deserialize IPC schema", e);
    }
  }

  public void registerParquet(String name, String path) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    registerParquet(nativeHandle, name, path);
  }

  @Override
  public void close() {
    if (nativeHandle != 0) {
      closeSessionContext(nativeHandle);
      nativeHandle = 0;
    }
  }

  private static native long createSessionContext();

  private static native long createDataFrame(long handle, String sql);

  private static native long createDataFrameFromProto(long handle, byte[] planBytes);

  private static native byte[] tableSchemaIpc(long handle, String tableName);

  private static native void registerParquet(long handle, String name, String path);

  private static native void closeSessionContext(long handle);
}
