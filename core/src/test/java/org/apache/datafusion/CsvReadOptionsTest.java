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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.protobuf.CsvReadOptionsProto;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;

class CsvReadOptionsTest {

  @Test
  void defaultsRoundTripThroughProto() throws InvalidProtocolBufferException {
    CsvReadOptionsProto p = CsvReadOptionsProto.parseFrom(new CsvReadOptions().toBytes());

    assertTrue(p.getHasHeader());
    assertEquals((int) ',', p.getDelimiter());
    assertEquals((int) '"', p.getQuote());
    assertEquals(".csv", p.getFileExtension());
    assertEquals(
        org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_UNCOMPRESSED,
        p.getFileCompressionType());

    assertFalse(p.hasTerminator());
    assertFalse(p.hasEscape());
    assertFalse(p.hasComment());
    assertFalse(p.hasNewlinesInValues());
    assertFalse(p.hasSchemaInferMaxRecords());
  }

  @Test
  void fullyConfiguredRoundTripsThroughProto() throws InvalidProtocolBufferException {
    CsvReadOptions opts =
        new CsvReadOptions()
            .hasHeader(false)
            .delimiter((byte) '|')
            .quote((byte) '\'')
            .terminator((byte) '\n')
            .escape((byte) '\\')
            .comment((byte) '#')
            .newlinesInValues(true)
            .schemaInferMaxRecords(10L)
            .fileExtension(".tsv")
            .fileCompressionType(FileCompressionType.GZIP);

    CsvReadOptionsProto p = CsvReadOptionsProto.parseFrom(opts.toBytes());

    assertFalse(p.getHasHeader());
    assertEquals((int) '|', p.getDelimiter());
    assertEquals((int) '\'', p.getQuote());
    assertEquals((int) '\n', p.getTerminator());
    assertEquals((int) '\\', p.getEscape());
    assertEquals((int) '#', p.getComment());
    assertTrue(p.getNewlinesInValues());
    assertEquals(10L, p.getSchemaInferMaxRecords());
    assertEquals(".tsv", p.getFileExtension());
    assertEquals(
        org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_GZIP,
        p.getFileCompressionType());
  }

  @Test
  void schemaIsHeldByReferenceAndNotInProto() {
    Schema schema =
        new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    CsvReadOptions opts = new CsvReadOptions().schema(schema);

    assertSame(schema, opts.schema());
  }

  @Test
  void allCompressionTypesMapThroughProto() throws InvalidProtocolBufferException {
    for (FileCompressionType t : FileCompressionType.values()) {
      CsvReadOptionsProto p =
          CsvReadOptionsProto.parseFrom(new CsvReadOptions().fileCompressionType(t).toBytes());
      assertEquals(
          "FILE_COMPRESSION_TYPE_" + t.name(),
          p.getFileCompressionType().name(),
          "mismatch for " + t);
    }
  }

  @Test
  void schemaInferMaxRecordsRejectsNegative() {
    // The proto wire field is uint64, so a negative long would be reinterpreted
    // as a huge unsigned value on the Rust side and silently expand schema
    // inference across the full dataset. Reject at the Java setter instead.
    CsvReadOptions opts = new CsvReadOptions();
    assertThrows(IllegalArgumentException.class, () -> opts.schemaInferMaxRecords(-1L));
    assertThrows(IllegalArgumentException.class, () -> opts.schemaInferMaxRecords(Long.MIN_VALUE));
  }
}
