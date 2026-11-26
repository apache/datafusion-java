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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Converts an Arrow schema (produced by the ADBC scan) into a Spark {@link StructType}, and plans
 * the source-side casts that make the scan emit Arrow that Spark's vectorized reader can consume.
 *
 * <p>An Arrow column must clear two independent gates:
 *
 * <ol>
 *   <li><b>Schema mapping</b> -- an Arrow type maps to some Spark {@code DataType} (used at {@code
 *       inferSchema}).
 *   <li><b>Vectorized read</b> -- Spark's {@code ArrowColumnVector} has an accessor for the vector.
 *       This gate is <em>narrower</em> than the first and is not a public API.
 * </ol>
 *
 * <p>Enumerating the two gates by hand lets them drift: a type can map to a Spark type yet have no
 * accessor (e.g. {@code FixedSizeList}/{@code LargeList} both map to {@code ArrayType} but only a
 * variable {@code ListVector} is readable), producing an {@code UNSUPPORTED_ARROWTYPE} at task time
 * rather than plan time. To avoid that, everything derives from one authority:
 *
 * <ul>
 *   <li>{@link #sparkConsumable} -- the read gate: {@code true} iff {@code ArrowColumnVector} has
 *       an accessor for the type. Mirrors the accessors in Spark 4.0's {@code
 *       ArrowColumnVector.initAccessor} (deliberately the narrower gate).
 *   <li>{@link #sparkTarget} -- the nearest consumable Arrow type for a field (identity if already
 *       consumable), recursing into children. Non-consumable leaves widen (unsigned -&gt; signed,
 *       Float16 -&gt; Float32, non-µs timestamp -&gt; µs, Date64 -&gt; Date32, Time -&gt; int,
 *       FixedSizeBinary -&gt; Binary) and non-consumable containers convert ({@code
 *       FixedSizeList}/{@code LargeList} -&gt; {@code List}).
 * </ul>
 *
 * <p>Then the reported Spark type, the cast decision, and the cast target all come from {@code
 * sparkTarget}, so the two gates cannot drift and adding a type is one case, not several. The cast
 * is pushed into the scan (see {@link SqlQuery}); {@link #toSparkSchema} asserts at plan time that
 * every target is consumable, so an unsupported type fails in planning with a clear message rather
 * than as an opaque executor crash.
 */
final class SchemaConverter {

  /**
   * Metadata flag set on a top-level Spark field whose source column needs a cast. Read by {@link
   * AdbcScanBuilder} to keep filter pushdown off these columns: a pushed predicate runs against the
   * pre-cast source column, so its literal would be in the wrong (source, not Spark) domain.
   */
  static final String CAST_METADATA_KEY = "org.apache.datafusion.spark.adbc.cast";

  private SchemaConverter() {}

  /** A projected output column: its name and, when a cast is required, the Arrow target type. */
  record ProjectionColumn(String name, String castType) {}

  static StructType toSparkSchema(Schema arrowSchema) {
    StructType struct = new StructType();
    for (Field field : arrowSchema.getFields()) {
      Field target = sparkTarget(field);
      // Plan-time guard: if a type cannot be normalized to something Spark can read, fail here
      // (naming the column) instead of crashing on an executor with UNSUPPORTED_ARROWTYPE.
      ensureConsumable(field.getName(), target);
      Metadata metadata =
          target == field
              ? Metadata.empty()
              : new MetadataBuilder().putBoolean(CAST_METADATA_KEY, true).build();
      struct = struct.add(field.getName(), mapConsumable(target), field.isNullable(), metadata);
    }
    return struct;
  }

  /**
   * Plan the projected columns for the pushed scan: the kept columns in output order, each with the
   * Arrow cast target (or {@code null} to pass through).
   *
   * @param schema the full source Arrow schema
   * @param projection kept column names in output order, or {@code null} for all columns
   */
  static List<ProjectionColumn> projectionColumns(Schema schema, List<String> projection) {
    List<Field> fields = new ArrayList<>();
    if (projection == null) {
      fields.addAll(schema.getFields());
    } else {
      for (String name : projection) {
        fields.add(schema.findField(name));
      }
    }
    List<ProjectionColumn> columns = new ArrayList<>(fields.size());
    for (Field field : fields) {
      columns.add(new ProjectionColumn(field.getName(), castTargetString(field)));
    }
    return columns;
  }

  /** Whether the column (recursively) is not something Spark's reader can consume as-is. */
  static boolean needsCast(Field field) {
    return sparkTarget(field) != field;
  }

  /** The Arrow type string for {@code arrow_cast}, or {@code null} if the column needs no cast. */
  static String castTargetString(Field field) {
    Field target = sparkTarget(field);
    return target == field ? null : render(target);
  }

  /**
   * A single Spark-readable column for a column-less scan (e.g. {@code count()}, whose projection
   * Catalyst prunes to empty). Such a scan only needs a row count, but the emitted stream must
   * still be Spark-native -- a bare {@code SELECT *} would return the raw, uncast schema and the
   * reader would fail on the first non-Spark-native column. Prefers a column that needs no cast
   * (cheapest); falls back to the first column with its cast applied when every column needs one.
   */
  static ProjectionColumn probeColumn(Schema schema) {
    for (Field field : schema.getFields()) {
      if (!needsCast(field)) {
        return new ProjectionColumn(field.getName(), null);
      }
    }
    Field first = schema.getFields().get(0);
    return new ProjectionColumn(first.getName(), castTargetString(first));
  }

  // --- The two-gate authority -----------------------------------------------

  /**
   * Whether Spark's vectorized {@code ArrowColumnVector} has an accessor for this Arrow type.
   * Mirrors {@code ArrowColumnVector.initAccessor} in Spark 4.0 -- deliberately the narrower gate,
   * so a type that maps to a Spark {@code DataType} but has no accessor (unsigned, Float16, non-µs
   * timestamp, Date64, {@code Time*}, {@code FixedSizeList}/{@code LargeList}, {@code
   * FixedSizeBinary}, {@code Interval}, {@code Dictionary}, ...) is reported as non-consumable.
   */
  static boolean sparkConsumable(ArrowType type) {
    if (type instanceof ArrowType.Bool) {
      return true;
    }
    if (type instanceof ArrowType.Int i) {
      return i.getIsSigned()
          && (i.getBitWidth() == 8
              || i.getBitWidth() == 16
              || i.getBitWidth() == 32
              || i.getBitWidth() == 64);
    }
    if (type instanceof ArrowType.FloatingPoint fp) {
      return fp.getPrecision() == FloatingPointPrecision.SINGLE
          || fp.getPrecision() == FloatingPointPrecision.DOUBLE;
    }
    if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
      return true;
    }
    if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
      return true; // FixedSizeBinary is NOT readable.
    }
    if (type instanceof ArrowType.Decimal d) {
      return d.getBitWidth() == 128; // Spark's DecimalVector is 128-bit; Decimal256 is not read.
    }
    if (type instanceof ArrowType.Date d) {
      return d.getUnit() == DateUnit.DAY; // Date64 (MILLISECOND) is not readable.
    }
    if (type instanceof ArrowType.Timestamp ts) {
      return ts.getUnit() == TimeUnit.MICROSECOND; // only µs, any timezone.
    }
    if (type instanceof ArrowType.Duration) {
      return true;
    }
    if (type instanceof ArrowType.Null) {
      return true;
    }
    // Only the variable-offset containers are readable (not FixedSizeList / LargeList).
    return type instanceof ArrowType.List
        || type instanceof ArrowType.Struct
        || type instanceof ArrowType.Map;
  }

  /**
   * The nearest Spark-consumable field for {@code field}, recursing into children. Returns the same
   * instance when nothing changes (so {@code sparkTarget(f) == f} means "no cast needed").
   */
  static Field sparkTarget(Field field) {
    ArrowType type = field.getType();
    ArrowType targetType = type;

    if (type instanceof ArrowType.Int i && !i.getIsSigned()) {
      targetType =
          switch (i.getBitWidth()) {
            case 8 -> new ArrowType.Int(16, true);
            case 16 -> new ArrowType.Int(32, true);
            case 32 -> new ArrowType.Int(64, true);
            case 64 -> new ArrowType.Decimal(20, 0, 128); // no lossless signed 64-bit target
            default -> type;
          };
    } else if (type instanceof ArrowType.FloatingPoint fp
        && fp.getPrecision() == FloatingPointPrecision.HALF) {
      targetType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (type instanceof ArrowType.Timestamp ts && ts.getUnit() != TimeUnit.MICROSECOND) {
      targetType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, ts.getTimezone());
    } else if (type instanceof ArrowType.Date d && d.getUnit() != DateUnit.DAY) {
      targetType = new ArrowType.Date(DateUnit.DAY);
    } else if (type instanceof ArrowType.Time t) {
      // Spark has no time-of-day accessor; carry the raw ticks as the matching-width signed int.
      targetType = new ArrowType.Int(t.getBitWidth() == 32 ? 32 : 64, true);
    } else if (type instanceof ArrowType.FixedSizeBinary) {
      targetType = new ArrowType.Binary();
    } else if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.LargeList) {
      // Spark reads ArrayType only from a variable ListVector.
      targetType = new ArrowType.List();
    }

    // Recurse into children (list element, struct fields, map entries), widening each.
    List<Field> children = field.getChildren();
    List<Field> targetChildren = new ArrayList<>(children.size());
    boolean childChanged = false;
    for (Field child : children) {
      Field targetChild = sparkTarget(child);
      childChanged |= targetChild != child;
      targetChildren.add(targetChild);
    }

    if (targetType == type && !childChanged) {
      return field;
    }
    FieldType ft = new FieldType(field.isNullable(), targetType, field.getDictionary());
    return new Field(field.getName(), ft, targetChildren);
  }

  /** Assert every node of a {@code sparkTarget} result is consumable, else fail with the column. */
  private static void ensureConsumable(String column, Field target) {
    if (!sparkConsumable(target.getType())) {
      throw new IllegalArgumentException(
          "column '"
              + column
              + "': Arrow type "
              + target.getType()
              + " has no Spark reader and "
              + "no supported cast; the connector cannot expose it to Spark");
    }
    for (Field child : target.getChildren()) {
      ensureConsumable(column, child);
    }
  }

  // --- Arrow (consumable) type -> Spark type ---------------------------------

  /** Map an already-{@link #sparkConsumable} field to its Spark {@link DataType}. */
  static DataType toSparkType(Field field) {
    Field target = sparkTarget(field);
    ensureConsumable(field.getName(), target);
    return mapConsumable(target);
  }

  private static DataType mapConsumable(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Bool) {
      return DataTypes.BooleanType;
    }
    if (type instanceof ArrowType.Int i) {
      return switch (i.getBitWidth()) {
        case 8 -> DataTypes.ByteType;
        case 16 -> DataTypes.ShortType;
        case 32 -> DataTypes.IntegerType;
        case 64 -> DataTypes.LongType;
        default -> throw unsupported(field);
      };
    }
    if (type instanceof ArrowType.FloatingPoint fp) {
      return fp.getPrecision() == FloatingPointPrecision.DOUBLE
          ? DataTypes.DoubleType
          : DataTypes.FloatType;
    }
    if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
      return DataTypes.StringType;
    }
    if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
      return DataTypes.BinaryType;
    }
    if (type instanceof ArrowType.Date) {
      return DataTypes.DateType;
    }
    if (type instanceof ArrowType.Timestamp ts) {
      return ts.getTimezone() == null ? DataTypes.TimestampNTZType : DataTypes.TimestampType;
    }
    if (type instanceof ArrowType.Decimal d) {
      return DataTypes.createDecimalType(d.getPrecision(), d.getScale());
    }
    if (type instanceof ArrowType.Null) {
      return DataTypes.NullType;
    }
    if (type instanceof ArrowType.Duration) {
      return DataTypes.createDayTimeIntervalType();
    }
    if (type instanceof ArrowType.List) {
      Field element = field.getChildren().get(0);
      return DataTypes.createArrayType(mapConsumable(element), element.isNullable());
    }
    if (type instanceof ArrowType.Struct) {
      List<StructField> children = new ArrayList<>();
      for (Field child : field.getChildren()) {
        children.add(
            DataTypes.createStructField(child.getName(), mapConsumable(child), child.isNullable()));
      }
      return DataTypes.createStructType(children);
    }
    if (type instanceof ArrowType.Map) {
      Field entries = field.getChildren().get(0);
      Field key = entries.getChildren().get(0);
      Field value = entries.getChildren().get(1);
      return DataTypes.createMapType(mapConsumable(key), mapConsumable(value), value.isNullable());
    }
    throw unsupported(field);
  }

  // --- Arrow (consumable) type -> arrow_cast type string ---------------------

  /**
   * Render an already-{@link #sparkConsumable} field as an {@code arrow_cast} type string (the
   * reversible {@code arrow::datatypes::DataType} display form DataFusion's {@code arrow_cast}
   * parses). Called only on {@link #sparkTarget} output, which is consumable by construction.
   */
  private static String render(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Bool) {
      return "Boolean";
    }
    if (type instanceof ArrowType.Int i) {
      return "Int" + i.getBitWidth();
    }
    if (type instanceof ArrowType.FloatingPoint fp) {
      return fp.getPrecision() == FloatingPointPrecision.DOUBLE ? "Float64" : "Float32";
    }
    if (type instanceof ArrowType.Utf8) {
      return "Utf8";
    }
    if (type instanceof ArrowType.LargeUtf8) {
      return "LargeUtf8";
    }
    if (type instanceof ArrowType.Binary) {
      return "Binary";
    }
    if (type instanceof ArrowType.LargeBinary) {
      return "LargeBinary";
    }
    if (type instanceof ArrowType.Date) {
      return "Date32";
    }
    if (type instanceof ArrowType.Timestamp ts) {
      return ts.getTimezone() == null
          ? "Timestamp(Microsecond)"
          : "Timestamp(Microsecond, \"" + ts.getTimezone() + "\")";
    }
    if (type instanceof ArrowType.Decimal d) {
      return "Decimal128(" + d.getPrecision() + ", " + d.getScale() + ")";
    }
    if (type instanceof ArrowType.Duration dur) {
      return "Duration(" + timeUnitName(dur.getUnit()) + ")";
    }
    if (type instanceof ArrowType.Null) {
      return "Null";
    }
    if (type instanceof ArrowType.List) {
      return "List(" + listChild(field.getChildren().get(0)) + ")";
    }
    if (type instanceof ArrowType.Struct) {
      StringBuilder sb = new StringBuilder("Struct(");
      List<Field> children = field.getChildren();
      for (int i = 0; i < children.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(structField(children.get(i)));
      }
      return sb.append(")").toString();
    }
    if (type instanceof ArrowType.Map m) {
      Field entries = field.getChildren().get(0);
      return "Map("
          + structField(entries)
          + ", "
          + (m.getKeysSorted() ? "sorted" : "unsorted")
          + ")";
    }
    throw unsupported(field);
  }

  /** {@code <nullability><type>[, field: 'name']} -- the list child form. */
  private static String listChild(Field field) {
    String rendered = nullability(field) + render(field);
    return "item".equals(field.getName())
        ? rendered
        : rendered + ", field: '" + field.getName() + "'";
  }

  /** {@code "name": <nullability><type>} -- the struct/map field form. */
  private static String structField(Field field) {
    return debugQuote(field.getName()) + ": " + nullability(field) + render(field);
  }

  private static String nullability(Field field) {
    return field.isNullable() ? "" : "non-null ";
  }

  private static String timeUnitName(TimeUnit unit) {
    return switch (unit) {
      case SECOND -> "Second";
      case MILLISECOND -> "Millisecond";
      case MICROSECOND -> "Microsecond";
      case NANOSECOND -> "Nanosecond";
    };
  }

  /** Reproduce Rust's {@code {:?}} string quoting used by the Arrow display form. */
  private static String debugQuote(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> sb.append(c);
      }
    }
    return sb.append("\"").toString();
  }

  private static IllegalArgumentException unsupported(Field field) {
    return new IllegalArgumentException(
        "unsupported Arrow type for column '" + field.getName() + "': " + field.getType());
  }
}
