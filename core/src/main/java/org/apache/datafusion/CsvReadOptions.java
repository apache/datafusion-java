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
 * Configuration knobs for CSV sources passed to {@link SessionContext#registerCsv(String, String,
 * CsvReadOptions)} and {@link SessionContext#readCsv(String, CsvReadOptions)}.
 *
 * <p>Mirrors a subset of DataFusion's {@code CsvReadOptions}. All setters return {@code this} for
 * fluent chaining. Defaults match the Rust struct: {@code hasHeader = true}, {@code delimiter =
 * ','}, {@code quote = '"'}, {@code fileExtension = ".csv"}, {@code fileCompressionType =
 * UNCOMPRESSED}, all other fields {@code null} (meaning the DataFusion default is used, or the
 * schema is inferred from the file).
 */
public final class CsvReadOptions {

  private boolean hasHeader = true;
  private byte delimiter = (byte) ',';
  private byte quote = (byte) '"';
  private Byte terminator;
  private Byte escape;
  private Byte comment;
  private Boolean newlinesInValues;
  private Long schemaInferMaxRecords;
  private String fileExtension = ".csv";
  private FileCompressionType fileCompressionType = FileCompressionType.UNCOMPRESSED;
  private Schema schema;

  public CsvReadOptions hasHeader(boolean v) {
    this.hasHeader = v;
    return this;
  }

  public CsvReadOptions delimiter(byte b) {
    this.delimiter = b;
    return this;
  }

  public CsvReadOptions quote(byte b) {
    this.quote = b;
    return this;
  }

  public CsvReadOptions terminator(byte b) {
    this.terminator = b;
    return this;
  }

  public CsvReadOptions escape(byte b) {
    this.escape = b;
    return this;
  }

  public CsvReadOptions comment(byte b) {
    this.comment = b;
    return this;
  }

  public CsvReadOptions newlinesInValues(boolean v) {
    this.newlinesInValues = v;
    return this;
  }

  public CsvReadOptions schemaInferMaxRecords(long n) {
    this.schemaInferMaxRecords = n;
    return this;
  }

  public CsvReadOptions fileExtension(String ext) {
    this.fileExtension = ext;
    return this;
  }

  public CsvReadOptions fileCompressionType(FileCompressionType t) {
    this.fileCompressionType = t;
    return this;
  }

  public CsvReadOptions schema(Schema schema) {
    this.schema = schema;
    return this;
  }

  byte[] toBytes() {
    org.apache.datafusion.protobuf.CsvReadOptionsProto.Builder b =
        org.apache.datafusion.protobuf.CsvReadOptionsProto.newBuilder()
            .setHasHeader(hasHeader)
            .setDelimiter(delimiter & 0xFF)
            .setQuote(quote & 0xFF)
            .setFileExtension(fileExtension)
            .setFileCompressionType(FileCompressionTypes.toProto(fileCompressionType));
    if (terminator != null) {
      b.setTerminator(terminator & 0xFF);
    }
    if (escape != null) {
      b.setEscape(escape & 0xFF);
    }
    if (comment != null) {
      b.setComment(comment & 0xFF);
    }
    if (newlinesInValues != null) {
      b.setNewlinesInValues(newlinesInValues);
    }
    if (schemaInferMaxRecords != null) {
      b.setSchemaInferMaxRecords(schemaInferMaxRecords);
    }
    return b.build().toByteArray();
  }

  Schema schema() {
    return schema;
  }
}
