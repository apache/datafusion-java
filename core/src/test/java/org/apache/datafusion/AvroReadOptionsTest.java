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
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.protobuf.AvroReadOptionsProto;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;

class AvroReadOptionsTest {

  @Test
  void defaultsRoundTripThroughProto() throws InvalidProtocolBufferException {
    AvroReadOptionsProto p = AvroReadOptionsProto.parseFrom(new AvroReadOptions().toBytes());
    assertEquals(".avro", p.getFileExtension());
  }

  @Test
  void fileExtensionRoundTripsThroughProto() throws InvalidProtocolBufferException {
    AvroReadOptionsProto p =
        AvroReadOptionsProto.parseFrom(new AvroReadOptions().fileExtension(".av").toBytes());
    assertEquals(".av", p.getFileExtension());
  }

  @Test
  void schemaIsHeldByReferenceAndNotInProto() {
    Schema schema =
        new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    AvroReadOptions opts = new AvroReadOptions().schema(schema);
    assertSame(schema, opts.schema());
  }
}
