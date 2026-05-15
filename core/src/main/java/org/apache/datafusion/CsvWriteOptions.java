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

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration knobs for writing CSV, passed to {@link DataFrame#writeCsv(String,
 * CsvWriteOptions)}.
 *
 * <p>Mirrors a subset of DataFusion's {@code DataFrameWriteOptions} and the writer-side {@code
 * CsvOptions}. All setters return {@code this} for fluent chaining. Defaults: every field {@code
 * null} or empty (meaning the DataFusion default is used).
 *
 * <p>Path semantics: when {@link #singleFileOutput(boolean)} is {@code true}, the path passed to
 * {@code writeCsv} is the literal output filename. When left unset (the default) and there are no
 * partition columns, the path is treated as a directory that DataFusion populates with one or more
 * part-files.
 *
 * <p>Compression reuses {@link CsvReadOptions.FileCompressionType} -- both the read and write sides
 * accept the same codec set ({@code UNCOMPRESSED}, {@code GZIP}, {@code BZIP2}, {@code XZ}, {@code
 * ZSTD}).
 */
public final class CsvWriteOptions {

  private Boolean singleFileOutput;
  private final List<String> partitionCols = new ArrayList<>();
  private Boolean hasHeader;
  private Byte delimiter;
  private Byte quote;
  private Byte escape;
  private String nullValue;
  private CsvReadOptions.FileCompressionType fileCompressionType;

  /**
   * When {@code true}, write to a single file at the supplied path. When left unset (the default)
   * and no partition columns are configured, the path is treated as a directory and DataFusion
   * writes one or more part-files.
   */
  public CsvWriteOptions singleFileOutput(boolean v) {
    this.singleFileOutput = v;
    return this;
  }

  /**
   * Hive-style partition columns. Each column listed here is removed from the data rows and encoded
   * into the directory layout (one subdirectory per distinct value). Mutually exclusive with {@link
   * #singleFileOutput(boolean)} -- DataFusion rejects the combination at write time.
   */
  public CsvWriteOptions partitionCols(String... cols) {
    this.partitionCols.clear();
    for (String c : cols) {
      this.partitionCols.add(c);
    }
    return this;
  }

  /** Whether to write a header row. Defaults to DataFusion's setting (typically {@code true}). */
  public CsvWriteOptions hasHeader(boolean v) {
    this.hasHeader = v;
    return this;
  }

  /** Field delimiter byte. Defaults to {@code ','}. */
  public CsvWriteOptions delimiter(byte b) {
    this.delimiter = b;
    return this;
  }

  /** Quote character byte. Defaults to {@code '"'}. */
  public CsvWriteOptions quote(byte b) {
    this.quote = b;
    return this;
  }

  /** Escape character byte. Defaults to none. */
  public CsvWriteOptions escape(byte b) {
    this.escape = b;
    return this;
  }

  /** String to write for SQL NULL values. Defaults to the empty string. */
  public CsvWriteOptions nullValue(String s) {
    this.nullValue = s;
    return this;
  }

  /** Output compression codec. Defaults to uncompressed. */
  public CsvWriteOptions fileCompressionType(CsvReadOptions.FileCompressionType t) {
    this.fileCompressionType = t;
    return this;
  }

  byte[] toBytes() {
    org.apache.datafusion.protobuf.CsvWriteOptionsProto.Builder b =
        org.apache.datafusion.protobuf.CsvWriteOptionsProto.newBuilder();
    if (singleFileOutput != null) {
      b.setSingleFileOutput(singleFileOutput);
    }
    b.addAllPartitionCols(partitionCols);
    if (hasHeader != null) {
      b.setHasHeader(hasHeader);
    }
    if (delimiter != null) {
      b.setDelimiter(delimiter & 0xFF);
    }
    if (quote != null) {
      b.setQuote(quote & 0xFF);
    }
    if (escape != null) {
      b.setEscape(escape & 0xFF);
    }
    if (nullValue != null) {
      b.setNullValue(nullValue);
    }
    if (fileCompressionType != null) {
      b.setFileCompressionType(toProto(fileCompressionType));
    }
    return b.build().toByteArray();
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
