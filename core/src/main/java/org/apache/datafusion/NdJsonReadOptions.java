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
 * Configuration knobs for newline-delimited JSON sources passed to {@link
 * SessionContext#registerJson(String, String, NdJsonReadOptions)} and {@link
 * SessionContext#readJson(String, NdJsonReadOptions)}.
 *
 * <p>Mirrors a subset of DataFusion's {@code NdJsonReadOptions}. All setters return {@code this}
 * for fluent chaining. Defaults match the Rust struct: {@code fileExtension = ".json"}, {@code
 * fileCompressionType = UNCOMPRESSED}; {@code schemaInferMaxRecords} unset (the DataFusion default
 * is used).
 *
 * <p>{@code FileCompressionType} is reused from {@link CsvReadOptions} since both formats accept
 * the same set of compressions.
 */
public final class NdJsonReadOptions {

  private String fileExtension = ".json";
  private CsvReadOptions.FileCompressionType fileCompressionType =
      CsvReadOptions.FileCompressionType.UNCOMPRESSED;
  private Long schemaInferMaxRecords;
  private Schema schema;

  public NdJsonReadOptions fileExtension(String ext) {
    this.fileExtension = ext;
    return this;
  }

  public NdJsonReadOptions fileCompressionType(CsvReadOptions.FileCompressionType t) {
    this.fileCompressionType = t;
    return this;
  }

  public NdJsonReadOptions schemaInferMaxRecords(long n) {
    this.schemaInferMaxRecords = n;
    return this;
  }

  public NdJsonReadOptions schema(Schema schema) {
    this.schema = schema;
    return this;
  }

  byte[] toBytes() {
    org.apache.datafusion.protobuf.NdJsonReadOptionsProto.Builder b =
        org.apache.datafusion.protobuf.NdJsonReadOptionsProto.newBuilder()
            .setFileExtension(fileExtension)
            .setFileCompressionType(toProto(fileCompressionType));
    if (schemaInferMaxRecords != null) {
      b.setSchemaInferMaxRecords(schemaInferMaxRecords);
    }
    return b.build().toByteArray();
  }

  Schema schema() {
    return schema;
  }

  private static org.apache.datafusion.protobuf.FileCompressionType toProto(
      CsvReadOptions.FileCompressionType t) {
    switch (t) {
      case UNCOMPRESSED:
        return org.apache.datafusion.protobuf.FileCompressionType
            .FILE_COMPRESSION_TYPE_UNCOMPRESSED;
      case GZIP:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_GZIP;
      case BZIP2:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_BZIP2;
      case XZ:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_XZ;
      case ZSTD:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_ZSTD;
      default:
        throw new IllegalArgumentException("unhandled FileCompressionType: " + t);
    }
  }
}
