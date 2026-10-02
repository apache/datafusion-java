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

package org.apache.datafusion.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.spark.SchemaConverter.ProjectionColumn;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the Arrow -> Spark schema mapping and the {@code arrow_cast} target planning: one
 * case per representable Arrow type, plus the cast-requiring types (unsigned, Float16, non-µs
 * timestamps) and nested forms.
 */
class SchemaConverterTest {

  private static Field nullable(String name, ArrowType type) {
    return Field.nullable(name, type);
  }

  private static Field nullable(String name, ArrowType type, Field... children) {
    return new Field(name, FieldType.nullable(type), List.of(children));
  }

  private static DataType spark(Field field) {
    return SchemaConverter.toSparkType(field);
  }

  // --- directly representable (A): mapped, no cast --------------------------

  @Test
  void primitivesMapWithoutCast() {
    assertEquals(DataTypes.ByteType, spark(nullable("c", new ArrowType.Int(8, true))));
    assertEquals(DataTypes.ShortType, spark(nullable("c", new ArrowType.Int(16, true))));
    assertEquals(DataTypes.IntegerType, spark(nullable("c", new ArrowType.Int(32, true))));
    assertEquals(DataTypes.LongType, spark(nullable("c", new ArrowType.Int(64, true))));
    assertEquals(
        DataTypes.DoubleType,
        spark(nullable("c", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    assertEquals(DataTypes.StringType, spark(nullable("c", ArrowType.Utf8.INSTANCE)));
    assertEquals(DataTypes.BooleanType, spark(nullable("c", ArrowType.Bool.INSTANCE)));

    assertNull(SchemaConverter.castTargetString(nullable("c", new ArrowType.Int(32, true))));
  }

  @Test
  void binaryMapsToBinary() {
    assertEquals(DataTypes.BinaryType, spark(nullable("c", ArrowType.Binary.INSTANCE)));
    assertEquals(DataTypes.BinaryType, spark(nullable("c", ArrowType.LargeBinary.INSTANCE)));
    assertEquals(DataTypes.BinaryType, spark(nullable("c", new ArrowType.FixedSizeBinary(16))));
    assertNull(SchemaConverter.castTargetString(nullable("c", ArrowType.Binary.INSTANCE)));
  }

  @Test
  void dateDecimalNull() {
    assertEquals(DataTypes.DateType, spark(nullable("c", new ArrowType.Date(DateUnit.DAY))));
    assertEquals(
        DataTypes.createDecimalType(10, 2),
        spark(nullable("c", new ArrowType.Decimal(10, 2, 128))));
    assertEquals(DataTypes.NullType, spark(nullable("c", ArrowType.Null.INSTANCE)));
  }

  @Test
  void microsecondTimestampNeedsNoCast() {
    Field ntz = nullable("c", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));
    assertEquals(DataTypes.TimestampNTZType, spark(ntz));
    assertNull(SchemaConverter.castTargetString(ntz));

    Field zoned = nullable("c", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
    assertEquals(DataTypes.TimestampType, spark(zoned));
    assertNull(SchemaConverter.castTargetString(zoned));
  }

  @Test
  void nestedListStructMap() {
    Field struct =
        nullable(
            "s",
            ArrowType.Struct.INSTANCE,
            nullable("first", ArrowType.Utf8.INSTANCE),
            nullable("second", ArrowType.Utf8.INSTANCE));
    Field listOfStruct = nullable("metadata", new ArrowType.List(), struct);

    DataType mapped = spark(listOfStruct);
    assertTrue(mapped instanceof org.apache.spark.sql.types.ArrayType);
    // A pure-(A) nested column needs no cast.
    assertNull(SchemaConverter.castTargetString(listOfStruct));
  }

  // --- cast required (B): mapped to widened type + arrow_cast target --------

  @Test
  void unsignedIntsWidenAndCast() {
    assertEquals(DataTypes.ShortType, spark(nullable("c", new ArrowType.Int(8, false))));
    assertEquals(DataTypes.IntegerType, spark(nullable("c", new ArrowType.Int(16, false))));
    assertEquals(DataTypes.LongType, spark(nullable("c", new ArrowType.Int(32, false))));
    assertEquals(
        DataTypes.createDecimalType(20, 0), spark(nullable("c", new ArrowType.Int(64, false))));

    assertEquals(
        "Int16", SchemaConverter.castTargetString(nullable("c", new ArrowType.Int(8, false))));
    assertEquals(
        "Int32", SchemaConverter.castTargetString(nullable("c", new ArrowType.Int(16, false))));
    assertEquals(
        "Int64", SchemaConverter.castTargetString(nullable("c", new ArrowType.Int(32, false))));
    assertEquals(
        "Decimal128(20, 0)",
        SchemaConverter.castTargetString(nullable("c", new ArrowType.Int(64, false))));
  }

  @Test
  void float16WidensToFloat() {
    Field f = nullable("c", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF));
    assertEquals(DataTypes.FloatType, spark(f));
    assertEquals("Float32", SchemaConverter.castTargetString(f));
  }

  @Test
  void nonMicrosecondTimestampRescales() {
    Field ns = nullable("t", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
    assertEquals(DataTypes.TimestampNTZType, spark(ns));
    assertEquals("Timestamp(Microsecond)", SchemaConverter.castTargetString(ns));

    Field zoned = nullable("t", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"));
    assertEquals(DataTypes.TimestampType, spark(zoned));
    assertEquals("Timestamp(Microsecond, \"UTC\")", SchemaConverter.castTargetString(zoned));
  }

  @Test
  void nestedListOfUnsignedCastsRecursively() {
    Field list =
        nullable("ids", new ArrowType.List(), nullable("item", new ArrowType.Int(16, false)));
    DataType mapped = spark(list);
    assertEquals(DataTypes.createArrayType(DataTypes.IntegerType, true), mapped);
    assertEquals("List(Int32)", SchemaConverter.castTargetString(list));
  }

  @Test
  void fixedSizeListCastsToVariableList() {
    // Spark reads ArrayType only from a variable ListVector, so a fixed-size list is always cast
    // to a variable list -- even when its element needs no cast.
    Field intFixed =
        new Field(
            "v",
            FieldType.nullable(new ArrowType.FixedSizeList(2)),
            List.of(nullable("item", new ArrowType.Int(32, true))));
    assertEquals(DataTypes.createArrayType(DataTypes.IntegerType, true), spark(intFixed));
    assertEquals("List(Int32)", SchemaConverter.castTargetString(intFixed));

    // The element is rendered cast-aware: FixedSizeList<Float16> -> List(Float32).
    Field halfFixed =
        new Field(
            "v",
            FieldType.nullable(new ArrowType.FixedSizeList(3)),
            List.of(nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))));
    assertEquals(DataTypes.createArrayType(DataTypes.FloatType, true), spark(halfFixed));
    assertEquals("List(Float32)", SchemaConverter.castTargetString(halfFixed));
  }

  @Test
  void largeListCastsToVariableList() {
    // LargeList maps to ArrayType but has no ArrowColumnVector accessor -> must cast to List.
    Field large =
        new Field(
            "v",
            FieldType.nullable(new ArrowType.LargeList()),
            List.of(nullable("item", new ArrowType.Int(32, true))));
    assertEquals(DataTypes.createArrayType(DataTypes.IntegerType, true), spark(large));
    assertEquals("List(Int32)", SchemaConverter.castTargetString(large));
  }

  @Test
  void fixedSizeBinaryCastsToBinary() {
    // FixedSizeBinary maps to BinaryType but has no accessor -> must cast to variable Binary.
    Field fsb = nullable("c", new ArrowType.FixedSizeBinary(16));
    assertEquals(DataTypes.BinaryType, spark(fsb));
    assertEquals("Binary", SchemaConverter.castTargetString(fsb));
  }

  @Test
  void date64CastsToDate32() {
    Field d64 = nullable("c", new ArrowType.Date(DateUnit.MILLISECOND));
    assertEquals(DataTypes.DateType, spark(d64));
    assertEquals("Date32", SchemaConverter.castTargetString(d64));

    // Date32 is consumable -> no cast.
    assertNull(SchemaConverter.castTargetString(nullable("c", new ArrowType.Date(DateUnit.DAY))));
  }

  @Test
  void sparkTargetIsAlwaysConsumable() {
    // The core invariant behind the plan-time guard: every widened target is Spark-consumable.
    List<Field> samples =
        List.of(
            nullable("a", new ArrowType.Int(8, false)),
            nullable("b", new ArrowType.Int(64, false)),
            nullable("c", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
            nullable("d", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
            nullable("e", new ArrowType.Date(DateUnit.MILLISECOND)),
            nullable("f", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
            nullable("g", new ArrowType.FixedSizeBinary(4)),
            new Field(
                "h",
                FieldType.nullable(new ArrowType.FixedSizeList(2)),
                List.of(
                    nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)))),
            new Field(
                "i",
                FieldType.nullable(new ArrowType.LargeList()),
                List.of(nullable("item", new ArrowType.Int(16, false)))),
            new Field(
                "j",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                List.of(nullable("x", new ArrowType.Int(32, false)))));
    for (Field f : samples) {
      assertConsumable(SchemaConverter.sparkTarget(f));
    }
  }

  private static void assertConsumable(Field field) {
    assertTrue(
        SchemaConverter.sparkConsumable(field.getType()),
        "sparkTarget produced a non-consumable type: " + field.getType());
    field.getChildren().forEach(SchemaConverterTest::assertConsumable);
  }

  @Test
  void unsupportedTypeFailsAtSchemaBuild() {
    // An Interval has no accessor and no supported cast -> fail fast at inferSchema, naming it.
    Schema schema =
        new Schema(List.of(Field.nullable("dur", new ArrowType.Interval(IntervalUnit.DAY_TIME))));
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaConverter.toSparkSchema(schema));
    assertTrue(e.getMessage().contains("dur"), e.getMessage());
  }

  // --- schema-level: metadata flag + projection planning -------------------

  @Test
  void castColumnsTaggedInSchemaMetadata() {
    Schema schema =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("channel", new ArrowType.Int(16, false)),
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null))));
    StructType spark = SchemaConverter.toSparkSchema(schema);

    assertTrue(spark.apply("id").metadata().isEmpty());
    assertTrue(spark.apply("channel").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(spark.apply("ts").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
  }

  @Test
  void probeColumnPrefersCastlessThenFallsBackToCast() {
    // A castless column is preferred (cheapest, no cast) for a column-less scan.
    Schema mixed =
        new Schema(
            List.of(
                Field.nullable("channel", new ArrowType.Int(16, false)),
                Field.nullable("id", new ArrowType.Int(64, true))));
    ProjectionColumn probe = SchemaConverter.probeColumn(mixed);
    assertEquals("id", probe.name());
    assertNull(probe.castType());

    // When every column needs a cast, the first column is used with its cast applied.
    Schema allCast =
        new Schema(
            List.of(
                Field.nullable("channel", new ArrowType.Int(16, false)),
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null))));
    ProjectionColumn casted = SchemaConverter.probeColumn(allCast);
    assertEquals("channel", casted.name());
    assertEquals("Int32", casted.castType());
  }

  @Test
  void projectionColumnsCarryCastTargets() {
    Schema schema =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("channel", new ArrowType.Int(16, false))));

    // All columns.
    List<ProjectionColumn> all = SchemaConverter.projectionColumns(schema, null);
    assertEquals(2, all.size());
    assertEquals("id", all.get(0).name());
    assertNull(all.get(0).castType());
    assertEquals("channel", all.get(1).name());
    assertEquals("Int32", all.get(1).castType());

    // Projected subset, order preserved.
    List<ProjectionColumn> pruned = SchemaConverter.projectionColumns(schema, List.of("channel"));
    assertEquals(1, pruned.size());
    assertEquals("channel", pruned.get(0).name());
    assertEquals("Int32", pruned.get(0).castType());
  }
}
