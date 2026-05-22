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

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.protobuf.AvroReadOptionsProto;

/**
 * Configuration knobs for Avro sources passed to {@link SessionContext#registerAvro(String, String,
 * AvroReadOptions)} and {@link SessionContext#readAvro(String, AvroReadOptions)}.
 *
 * <p>Mirrors the subset of DataFusion's {@code AvroReadOptions} that maps onto the Java surface
 * today: {@code fileExtension} (default {@code ".avro"}) and an explicit Arrow {@code schema} that
 * bypasses on-read schema inference. {@code tablePartitionCols} is intentionally deferred -- no
 * other Java reader exposes Hive-style partitioning yet.
 *
 * <p>Avro carries its own per-block compression (snappy, deflate, bzip2, xz, zstandard) inside the
 * object container itself, negotiated when the file is written, so unlike CSV / NDJSON there is no
 * {@code FileCompressionType} setter.
 */
public final class AvroReadOptions {

  private String fileExtension = ".avro";
  private Schema schema;

  public AvroReadOptions fileExtension(String ext) {
    this.fileExtension = ext;
    return this;
  }

  public AvroReadOptions schema(Schema schema) {
    this.schema = schema;
    return this;
  }

  byte[] toBytes() {
    return AvroReadOptionsProto.newBuilder().setFileExtension(fileExtension).build().toByteArray();
  }

  Schema schema() {
    return schema;
  }
}
