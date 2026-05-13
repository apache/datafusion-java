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

/**
 * Configuration knobs for parquet sources passed to {@link SessionContext#registerParquet(String,
 * String, ParquetReadOptions)} and {@link SessionContext#readParquet(String, ParquetReadOptions)}.
 *
 * <p>Mirrors a subset of DataFusion's {@code ParquetReadOptions}. All setters return {@code this}
 * for fluent chaining. Defaults: {@code fileExtension = ".parquet"}; all other fields {@code null}
 * (meaning the SessionConfig default is used, or the schema is inferred from the file).
 */
public final class ParquetReadOptions {

  private String fileExtension = ".parquet";
  private Boolean parquetPruning;
  private Boolean skipMetadata;
  private Long metadataSizeHint;
  private Schema schema;

  public ParquetReadOptions fileExtension(String ext) {
    this.fileExtension = ext;
    return this;
  }

  public ParquetReadOptions parquetPruning(boolean v) {
    this.parquetPruning = v;
    return this;
  }

  public ParquetReadOptions skipMetadata(boolean v) {
    this.skipMetadata = v;
    return this;
  }

  public ParquetReadOptions metadataSizeHint(long bytes) {
    this.metadataSizeHint = bytes;
    return this;
  }

  public ParquetReadOptions schema(Schema schema) {
    this.schema = schema;
    return this;
  }

  byte[] toBytes() {
    org.apache.datafusion.protobuf.ParquetReadOptionsProto.Builder b =
        org.apache.datafusion.protobuf.ParquetReadOptionsProto.newBuilder()
            .setFileExtension(fileExtension);
    if (parquetPruning != null) {
      b.setParquetPruning(parquetPruning);
    }
    if (skipMetadata != null) {
      b.setSkipMetadata(skipMetadata);
    }
    if (metadataSizeHint != null) {
      b.setMetadataSizeHint(metadataSizeHint);
    }
    return b.build().toByteArray();
  }

  Schema schema() {
    return schema;
  }
}
