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

package org.apache.datafusion.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import datafusion_common.DatafusionCommon;

class SchemaConverterTest {

  @Test
  void roundTripsPrimitivesAndMetadata() {
    Map<String, String> schemaMeta = new HashMap<>();
    schemaMeta.put("origin", "test");
    Map<String, String> fieldMeta = new HashMap<>();
    fieldMeta.put("ns", "demo");

    List<Field> fields =
        Arrays.asList(
            new Field("a_bool", FieldType.nullable(ArrowType.Bool.INSTANCE), null),
            new Field("a_i32", FieldType.notNullable(new ArrowType.Int(32, true)), null),
            new Field("a_i64", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("a_u32", FieldType.nullable(new ArrowType.Int(32, false)), null),
            new Field(
                "a_f64",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null),
            new Field("a_str", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("a_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
            new Field(
                "a_with_meta",
                new FieldType(true, new ArrowType.Int(8, true), null, fieldMeta),
                null));

    Schema original = new Schema(fields, schemaMeta);

    DatafusionCommon.Schema proto = SchemaConverter.toProto(original);
    Schema roundTripped = SchemaConverter.fromProto(proto);

    assertEquals(original, roundTripped);
  }

  @Test
  void decimalPreservesPrecisionAndScale() {
    Schema original =
        new Schema(
            List.of(
                new Field(
                    "amount",
                    FieldType.nullable(new ArrowType.Decimal(18, 5, 128)),
                    null)));

    DatafusionCommon.Schema proto = SchemaConverter.toProto(original);
    Schema roundTripped = SchemaConverter.fromProto(proto);

    assertEquals(original, roundTripped);
    ArrowType.Decimal d = (ArrowType.Decimal) roundTripped.getFields().get(0).getType();
    assertEquals(18, d.getPrecision());
    assertEquals(5, d.getScale());
    assertEquals(128, d.getBitWidth());
  }

  @Test
  void unsupportedTypeRaisesUnsupportedOperationException() {
    Field listField =
        new Field(
            "nested",
            FieldType.nullable(new ArrowType.List()),
            List.of(new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    Schema original = new Schema(List.of(listField));

    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> SchemaConverter.toProto(original));
    assertTrue(
        ex.getMessage().contains("List"),
        "exception message should name the unsupported type, was: " + ex.getMessage());
  }
}
