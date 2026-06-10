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

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.spark.sql.types._

/**
 * Arrow Schema → Spark StructType converter.
 *
 * The reported Spark schema MUST be one whose runtime ArrowColumnVector accessor Spark can pick
 * for the underlying Arrow vector. Spark 3.5's `ArrowColumnVector` supports the following
 * accessors: Boolean, Byte, Short, Int, Long, Float, Double, Decimal, Date, Timestamp,
 * TimestampNTZ, Duration (DayTimeInterval), String, LargeString, Binary, Array, Map, Struct,
 * Null. No unsigned-int or Time accessor exists; we surface a clear error at schema discovery
 * for those — the alternative is silent corruption.
 *
 * The widening cdylib (connector-core/native/) inserts a `WideningTableProvider` upstream of the
 * Spark reader that casts unsupported types kernel-side (UInt*→signed wider, Float16→Float32,
 * non-µs Timestamp→µs Timestamp, Time→Int) so Spark only ever sees compatible Arrow types.
 */
object ArrowToSparkSchema {

  def toSparkSchema(schema: Schema): StructType =
    StructType(schema.getFields.asScala.toSeq.map(toSparkField))

  private def toSparkField(f: Field): StructField = {
    val dt =
      Option(f.getDictionary) match {
        case Some(_) =>
          unsupported(f, "dictionary-encoded fields (need dictionary value schema in JNI)")
        case None => toSparkType(f)
      }
    StructField(f.getName, dt, f.isNullable)
  }

  private def toSparkType(f: Field): DataType = f.getType match {
    case _: ArrowType.Bool => BooleanType

    case t: ArrowType.Int =>
      (t.getBitWidth, t.getIsSigned) match {
        case (8, true) => ByteType
        case (16, true) => ShortType
        case (32, true) => IntegerType
        case (64, true) => LongType
        case (bits, false) =>
          unsupported(
            f,
            s"unsigned integer UInt$bits (Spark ArrowColumnVector has no unsigned accessor; " +
              "widening cdylib casts these before Spark sees them — this branch indicates the " +
              "WideningTableProvider was bypassed)"
          )
        case (bits, signed) => unsupported(f, s"Int(bits=$bits, signed=$signed)")
      }

    case t: ArrowType.FloatingPoint =>
      t.getPrecision match {
        case FloatingPointPrecision.HALF =>
          unsupported(f, "Float16 (widening cdylib must cast to Float32 before Spark)")
        case FloatingPointPrecision.SINGLE => FloatType
        case FloatingPointPrecision.DOUBLE => DoubleType
        case other => unsupported(f, s"FloatingPoint($other)")
      }

    case _: ArrowType.Utf8 => StringType
    case _: ArrowType.LargeUtf8 => StringType
    case _: ArrowType.Binary => BinaryType
    case _: ArrowType.LargeBinary => BinaryType
    case _: ArrowType.FixedSizeBinary => BinaryType

    case d: ArrowType.Date =>
      d.getUnit match {
        case DateUnit.DAY | DateUnit.MILLISECOND => DateType
        case other => unsupported(f, s"Date($other)")
      }

    case t: ArrowType.Timestamp =>
      val _unused = t.getUnit
      if (t.getTimezone == null) TimestampNTZType else TimestampType

    case ti: ArrowType.Time =>
      unsupported(
        f,
        s"Time(${ti.getUnit}, ${ti.getBitWidth}-bit) — Spark has no time-of-day type"
      )

    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)

    case _: ArrowType.Null => NullType

    case _: ArrowType.Duration => DayTimeIntervalType()

    case iv: ArrowType.Interval =>
      iv.getUnit match {
        case IntervalUnit.YEAR_MONTH => YearMonthIntervalType()
        case IntervalUnit.DAY_TIME => DayTimeIntervalType()
        case IntervalUnit.MONTH_DAY_NANO =>
          unsupported(f, "Interval(MONTH_DAY_NANO) — no clean Spark equivalent")
      }

    case _: ArrowType.Struct =>
      StructType(f.getChildren.asScala.toSeq.map(toSparkField))

    case _: ArrowType.List =>
      val child = f.getChildren.get(0)
      ArrayType(toSparkType(child), containsNull = child.isNullable)
    case _: ArrowType.LargeList =>
      val child = f.getChildren.get(0)
      ArrayType(toSparkType(child), containsNull = child.isNullable)
    case _: ArrowType.FixedSizeList =>
      val child = f.getChildren.get(0)
      ArrayType(toSparkType(child), containsNull = child.isNullable)

    case _: ArrowType.Map =>
      val entries = f.getChildren.get(0)
      val keyValue = entries.getChildren
      val keyField = keyValue.get(0)
      val valueField = keyValue.get(1)
      MapType(toSparkType(keyField), toSparkType(valueField), valueField.isNullable)

    case _: ArrowType.Union =>
      unsupported(f, "Union (Spark has no equivalent)")

    case other => unsupported(f, s"$other")
  }

  private def unsupported(f: Field, detail: String): Nothing =
    throw new UnsupportedOperationException(
      s"Column '${f.getName}': $detail"
    )
}
