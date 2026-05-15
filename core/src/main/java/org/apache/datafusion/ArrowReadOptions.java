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
import org.apache.datafusion.protobuf.ArrowReadOptionsProto;

/**
 * Configuration knobs for Arrow IPC sources passed to {@link SessionContext#registerArrow(String,
 * String, ArrowReadOptions)} and {@link SessionContext#readArrow(String, ArrowReadOptions)}.
 *
 * <p>Mirrors the subset of DataFusion's {@code ArrowReadOptions} that maps onto the Java surface
 * today: {@code fileExtension} (default {@code ".arrow"}) and an explicit Arrow {@code schema} that
 * bypasses on-read schema inference. {@code tablePartitionCols} is intentionally deferred --
 * neither Parquet nor CSV expose Hive-style partitioning on the Java side yet.
 *
 * <p>Arrow IPC files carry their own body compression (LZ4_FRAME / ZSTD per-buffer) inside the file
 * format itself, so unlike CSV / NDJSON there is no {@code FileCompressionType} setter.
 */
public final class ArrowReadOptions {

  private String fileExtension = ".arrow";
  private Schema schema;

  public ArrowReadOptions fileExtension(String ext) {
    this.fileExtension = ext;
    return this;
  }

  public ArrowReadOptions schema(Schema schema) {
    this.schema = schema;
    return this;
  }

  byte[] toBytes() {
    return ArrowReadOptionsProto.newBuilder().setFileExtension(fileExtension).build().toByteArray();
  }

  Schema schema() {
    return schema;
  }
}
