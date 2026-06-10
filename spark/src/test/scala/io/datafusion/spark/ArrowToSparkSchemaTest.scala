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

package io.datafusion.spark

import java.util.Collections

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class ArrowToSparkSchemaTest extends AnyFunSuite {

  private def primField(name: String, t: ArrowType, nullable: Boolean = true): Field =
    new Field(name, new FieldType(nullable, t, /*dict=*/ null), Collections.emptyList())

  test("signed ints map to matching Spark int types") {
    val arrow = new Schema(
      java.util.Arrays.asList(
        primField("i8", new ArrowType.Int(8, true)),
        primField("i16", new ArrowType.Int(16, true)),
        primField("i32", new ArrowType.Int(32, true)),
        primField("i64", new ArrowType.Int(64, true))
      )
    )
    val s = ArrowToSparkSchema.toSparkSchema(arrow)
    assert(s.fields(0).dataType == ByteType)
    assert(s.fields(1).dataType == ShortType)
    assert(s.fields(2).dataType == IntegerType)
    assert(s.fields(3).dataType == LongType)
  }

  test("unsigned ints are rejected with a clear error") {
    val arrow = new Schema(
      java.util.Arrays.asList(primField("u32", new ArrowType.Int(32, false)))
    )
    val ex = intercept[UnsupportedOperationException](ArrowToSparkSchema.toSparkSchema(arrow))
    assert(ex.getMessage.contains("u32"))
    assert(ex.getMessage.toLowerCase.contains("unsigned"))
  }

  test("timestamps split on timezone presence") {
    val withTz = primField("t_utc", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))
    val noTz = primField("t_local", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))
    val s = ArrowToSparkSchema.toSparkSchema(
      new Schema(java.util.Arrays.asList(withTz, noTz))
    )
    assert(s.fields(0).dataType == TimestampType)
    assert(s.fields(1).dataType == TimestampNTZType)
  }

  test("decimal preserves precision and scale") {
    val s = ArrowToSparkSchema.toSparkSchema(
      new Schema(java.util.Arrays.asList(primField("d", new ArrowType.Decimal(18, 4, 128))))
    )
    assert(s.fields(0).dataType == DecimalType(18, 4))
  }

  test("Time and Float16 are rejected (no Spark accessor)") {
    intercept[UnsupportedOperationException] {
      ArrowToSparkSchema.toSparkSchema(
        new Schema(java.util.Arrays.asList(primField("t", new ArrowType.Time(TimeUnit.MICROSECOND, 64))))
      )
    }
    intercept[UnsupportedOperationException] {
      ArrowToSparkSchema.toSparkSchema(
        new Schema(java.util.Arrays.asList(primField("h", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))))
      )
    }
  }

  test("list element nullability propagates") {
    val child =
      new Field(
        "el",
        new FieldType(/*nullable=*/ true, new ArrowType.Int(32, true), null),
        Collections.emptyList()
      )
    val listField = new Field(
      "xs",
      new FieldType(true, new ArrowType.List(), null),
      java.util.Arrays.asList(child)
    )
    val s = ArrowToSparkSchema.toSparkSchema(
      new Schema(java.util.Arrays.asList(listField))
    )
    assert(s.fields(0).dataType == ArrayType(IntegerType, containsNull = true))
  }
}
