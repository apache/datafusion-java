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

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.protobuf.NdJsonReadOptionsProto;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;

class NdJsonReadOptionsTest {

  @Test
  void defaultsRoundTripThroughProto() throws InvalidProtocolBufferException {
    NdJsonReadOptionsProto p = NdJsonReadOptionsProto.parseFrom(new NdJsonReadOptions().toBytes());

    assertEquals(".json", p.getFileExtension());
    assertEquals(
        org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_UNCOMPRESSED,
        p.getFileCompressionType());
    assertFalse(p.hasSchemaInferMaxRecords());
  }

  @Test
  void fullyConfiguredRoundTripsThroughProto() throws InvalidProtocolBufferException {
    NdJsonReadOptions opts =
        new NdJsonReadOptions()
            .fileExtension(".ndjson")
            .fileCompressionType(FileCompressionType.GZIP)
            .schemaInferMaxRecords(50L);

    NdJsonReadOptionsProto p = NdJsonReadOptionsProto.parseFrom(opts.toBytes());

    assertEquals(".ndjson", p.getFileExtension());
    assertEquals(
        org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_GZIP,
        p.getFileCompressionType());
    assertEquals(50L, p.getSchemaInferMaxRecords());
  }

  @Test
  void schemaIsHeldByReferenceAndNotInProto() {
    Schema schema =
        new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    NdJsonReadOptions opts = new NdJsonReadOptions().schema(schema);

    assertSame(schema, opts.schema());
  }

  @Test
  void allCompressionTypesMapThroughProto() throws InvalidProtocolBufferException {
    for (FileCompressionType t : FileCompressionType.values()) {
      NdJsonReadOptionsProto p =
          NdJsonReadOptionsProto.parseFrom(
              new NdJsonReadOptions().fileCompressionType(t).toBytes());
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
    NdJsonReadOptions opts = new NdJsonReadOptions();
    assertThrows(IllegalArgumentException.class, () -> opts.schemaInferMaxRecords(-1L));
    assertThrows(IllegalArgumentException.class, () -> opts.schemaInferMaxRecords(Long.MIN_VALUE));
  }
}
