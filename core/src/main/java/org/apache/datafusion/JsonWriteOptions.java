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
 * Configuration knobs for writing JSON, passed to {@link DataFrame#writeJson(String,
 * JsonWriteOptions)}.
 *
 * <p>Mirrors a subset of DataFusion's {@code DataFrameWriteOptions} and the writer-side {@code
 * JsonOptions}. All setters return {@code this} for fluent chaining. Defaults: every field {@code
 * null} or empty (meaning the DataFusion default is used).
 *
 * <p>Path semantics: when {@link #singleFileOutput(boolean)} is {@code true}, the path passed to
 * {@code writeJson} is the literal output filename. When left unset (the default) and there are no
 * partition columns, the path is treated as a directory that DataFusion populates with one or more
 * part-files.
 *
 * <p>The output is always newline-delimited JSON (NDJSON). DataFusion's JSON writer does not emit
 * the bracketed array form, so there is no toggle for it here.
 *
 * <p>Compression reuses {@link FileCompressionType} -- the same codec set ({@code UNCOMPRESSED},
 * {@code GZIP}, {@code BZIP2}, {@code XZ}, {@code ZSTD}) the read side and the CSV writer accept.
 */
public final class JsonWriteOptions {

  private Boolean singleFileOutput;
  private final List<String> partitionCols = new ArrayList<>();
  private FileCompressionType fileCompressionType;

  /**
   * When {@code true}, write to a single file at the supplied path. When left unset (the default)
   * and no partition columns are configured, the path is treated as a directory and DataFusion
   * writes one or more part-files.
   */
  public JsonWriteOptions singleFileOutput(boolean v) {
    this.singleFileOutput = v;
    return this;
  }

  /**
   * Hive-style partition columns. Each column listed here is removed from the data rows and encoded
   * into the directory layout (one subdirectory per distinct value). Mutually exclusive with {@link
   * #singleFileOutput(boolean)} -- DataFusion rejects the combination at write time.
   */
  public JsonWriteOptions partitionCols(String... cols) {
    this.partitionCols.clear();
    for (String c : cols) {
      this.partitionCols.add(c);
    }
    return this;
  }

  /** Output compression codec. Defaults to uncompressed. */
  public JsonWriteOptions fileCompressionType(FileCompressionType t) {
    this.fileCompressionType = t;
    return this;
  }

  byte[] toBytes() {
    org.apache.datafusion.protobuf.JsonWriteOptionsProto.Builder b =
        org.apache.datafusion.protobuf.JsonWriteOptionsProto.newBuilder();
    if (singleFileOutput != null) {
      b.setSingleFileOutput(singleFileOutput);
    }
    b.addAllPartitionCols(partitionCols);
    if (fileCompressionType != null) {
      b.setFileCompressionType(FileCompressionTypes.toProto(fileCompressionType));
    }
    return b.build().toByteArray();
  }
}
