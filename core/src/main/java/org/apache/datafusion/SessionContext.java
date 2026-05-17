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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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

  SessionContext(byte[] optionsBytes) {
    this.nativeHandle = createSessionContextWithOptions(optionsBytes);
    if (this.nativeHandle == 0) {
      throw new RuntimeException("Failed to create native SessionContext");
    }
  }

  /** Start configuring a {@link SessionContext}. */
  public static SessionContextBuilder builder() {
    return new SessionContextBuilder();
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
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize IPC schema", e);
    }
  }

  public void registerCsv(String name, String path) {
    registerCsv(name, path, new CsvReadOptions());
  }

  /**
   * Register a CSV file (or directory of CSV files) as a table with the supplied {@link
   * CsvReadOptions}.
   *
   * @throws RuntimeException if registration fails (path not found, schema inference error, etc.).
   */
  public void registerCsv(String name, String path, CsvReadOptions options) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    registerCsvWithOptions(
        nativeHandle,
        name,
        path,
        options.toBytes(),
        options.schema() != null ? serializeSchemaIpc(options.schema()) : null);
  }

  /** Read a CSV file as a {@link DataFrame} without registering it. */
  public DataFrame readCsv(String path) {
    return readCsv(path, new CsvReadOptions());
  }

  /**
   * Read a CSV file as a {@link DataFrame} with the supplied {@link CsvReadOptions}.
   *
   * @throws RuntimeException if the read fails.
   */
  public DataFrame readCsv(String path, CsvReadOptions options) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    long dfHandle =
        readCsvWithOptions(
            nativeHandle,
            path,
            options.toBytes(),
            options.schema() != null ? serializeSchemaIpc(options.schema()) : null);
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
        options.toBytes(),
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
            options.toBytes(),
            options.schema() != null ? serializeSchemaIpc(options.schema()) : null);
    return new DataFrame(dfHandle);
  }

  /**
   * Register a Java-implemented scalar UDF. After registration, the function can be invoked by SQL
   * via the UDF's name or referenced in DataFusion plans deserialised with {@link #fromProto}.
   *
   * <p>The UDF is registered with an exact signature: the runtime will reject calls whose argument
   * types do not match the declared {@link ScalarFunction#argTypes()} exactly.
   *
   * @throws RuntimeException if registration fails (e.g., name already registered with an
   *     incompatible signature, schema serialisation failure).
   */
  public void registerUdf(ScalarUdf udf) {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SessionContext is closed");
    }
    java.util.Objects.requireNonNull(udf, "udf");
    ScalarFunction impl = udf.impl();
    String name = udf.name();
    ArrowType returnType = udf.returnType();
    List<ArrowType> argTypes = udf.argTypes();
    Volatility volatility = udf.volatility();
    List<Field> fields = new ArrayList<>(argTypes.size() + 1);
    fields.add(new Field("return", FieldType.nullable(returnType), null));
    for (int i = 0; i < argTypes.size(); i++) {
      fields.add(new Field("arg" + i, FieldType.nullable(argTypes.get(i)), null));
    }
    Schema signatureSchema = new Schema(fields);
    byte[] signatureBytes = serializeSchemaIpc(signatureSchema);
    registerScalarUdf(nativeHandle, name, signatureBytes, volatility.code(), impl);
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

  private static native long createSessionContextWithOptions(byte[] optionsBytes);

  private static native long createDataFrame(long handle, String sql);

  private static native long createDataFrameFromProto(long handle, byte[] planBytes);

  private static native byte[] tableSchemaIpc(long handle, String tableName);

  private static native void registerParquetWithOptions(
      long handle, String name, String path, byte[] optionsBytes, byte[] schemaIpcBytes);

  private static native long readParquetWithOptions(
      long handle, String path, byte[] optionsBytes, byte[] schemaIpcBytes);

  private static native void registerCsvWithOptions(
      long handle, String name, String path, byte[] optionsBytes, byte[] schemaIpcBytes);

  private static native long readCsvWithOptions(
      long handle, String path, byte[] optionsBytes, byte[] schemaIpcBytes);

  private static native void closeSessionContext(long handle);

  private static native void registerScalarUdf(
      long handle, String name, byte[] signatureSchemaBytes, byte volatility, ScalarFunction impl);
}
