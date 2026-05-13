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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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

  public void registerParquet(String name, String path) {
    registerParquet(name, path, new ParquetReadOptions());
  }

  /**
   * Register a parquet file as a table with the supplied {@link ParquetReadOptions}.
   *
   * @throws RuntimeException if registration fails (path not found, schema mismatch, etc.).
   */
  public void registerParquet(String name, String path, ParquetReadOptions options) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    registerParquetWithOptions(
        nativeHandle,
        name,
        path,
        options.fileExtension(),
        options.parquetPruning() != null,
        options.parquetPruning() != null && options.parquetPruning(),
        options.skipMetadata() != null,
        options.skipMetadata() != null && options.skipMetadata(),
        options.metadataSizeHint() != null ? options.metadataSizeHint() : -1L,
        options.schema() != null ? serializeSchemaIpc(options.schema()) : null);
  }

  /** Read a parquet file as a {@link DataFrame} without registering it. */
  public DataFrame readParquet(String path) {
    return readParquet(path, new ParquetReadOptions());
  }

  /**
   * Read a parquet file as a {@link DataFrame} with the supplied {@link ParquetReadOptions}.
   *
   * @throws RuntimeException if the read fails.
   */
  public DataFrame readParquet(String path, ParquetReadOptions options) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    long dfHandle =
        readParquetWithOptions(
            nativeHandle,
            path,
            options.fileExtension(),
            options.parquetPruning() != null,
            options.parquetPruning() != null && options.parquetPruning(),
            options.skipMetadata() != null,
            options.skipMetadata() != null && options.skipMetadata(),
            options.metadataSizeHint() != null ? options.metadataSizeHint() : -1L,
            options.schema() != null ? serializeSchemaIpc(options.schema()) : null);
    return new DataFrame(dfHandle);
  }

  private static byte[] serializeSchemaIpc(Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
      writer.start();
      writer.end();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize Arrow schema for JNI", e);
    }
    return baos.toByteArray();
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

  private static native void registerParquetWithOptions(
      long handle,
      String name,
      String path,
      String fileExtension,
      boolean parquetPruningSet,
      boolean parquetPruningValue,
      boolean skipMetadataSet,
      boolean skipMetadataValue,
      long metadataSizeHint,
      byte[] schemaIpcBytes);

  private static native long readParquetWithOptions(
      long handle,
      String path,
      String fileExtension,
      boolean parquetPruningSet,
      boolean parquetPruningValue,
      boolean skipMetadataSet,
      boolean skipMetadataValue,
      long metadataSizeHint,
      byte[] schemaIpcBytes);

  private static native void closeSessionContext(long handle);
}
