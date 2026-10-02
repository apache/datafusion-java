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
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;

/**
 * Builds the Substrait plan pushed to the ADBC statement ({@code setSubstraitPlan}).
 *
 * <p>Shape: {@code NamedScan -> [Filter] -> [Project(emit)] -> [Fetch]}. The plan is built with
 * substrait-java's {@link SubstraitBuilder}, and every pushed predicate resolves against the
 * <em>standard</em> Substrait function catalog ({@link
 * DefaultExtensionCatalog#FUNCTIONS_COMPARISON} / {@link
 * DefaultExtensionCatalog#FUNCTIONS_BOOLEAN}). That is the key to interop: those are the function
 * URIs/signatures DataFusion's Substrait consumer ({@code from_substrait_plan}) recognizes, so we
 * never emit a custom extension the two sides could disagree on. Anything outside the whitelist in
 * {@link #canPush} is left to Spark.
 *
 * <p>Pushdown is carried as typed Substrait rather than a SQL string, so literal fidelity
 * (float/decimal/NaN/null-safe) is not lost in a text round-trip.
 */
final class SubstraitPlan {

  /** Load the standard extension declarations once; immutable and shareable. */
  private static final SimpleExtension.ExtensionCollection EXTENSIONS =
      SimpleExtension.loadDefaults();

  /** Predicates produce a (nullable, three-valued) boolean. */
  private static final Type BOOL = TypeCreator.NULLABLE.BOOLEAN;

  private SubstraitPlan() {}

  /**
   * Build the scan plan with the given pushdown.
   *
   * @param table the table name the provider exposes
   * @param schema the table's full Arrow schema
   * @param projection kept column names (in output order), or {@code null} for all columns
   * @param filters the pushed predicates (must all satisfy {@link #canPush}); ANDed together
   * @param limit an optional row limit applied after filtering
   */
  static byte[] build(
      String table,
      Schema schema,
      List<String> projection,
      List<Filter> filters,
      OptionalLong limit) {
    SubstraitBuilder b = new SubstraitBuilder(EXTENSIONS);

    List<String> names = new ArrayList<>(schema.getFields().size());
    List<Type> types = new ArrayList<>(schema.getFields().size());
    Map<String, Integer> columnIndex = new java.util.HashMap<>();
    int i = 0;
    for (Field field : schema.getFields()) {
      names.add(field.getName());
      types.add(toSubstraitType(field));
      columnIndex.put(field.getName(), i++);
    }

    NamedScan scan = b.namedScan(List.of(table), names, types);
    Rel rel = scan;

    if (!filters.isEmpty()) {
      rel = b.filter(input -> conjunction(b, input, columnIndex, filters), rel);
    }

    List<String> outputNames = names;
    if (projection != null && !projection.equals(names)) {
      List<Integer> keep = projection.stream().map(columnIndex::get).collect(Collectors.toList());
      int inputWidth = names.size();
      rel =
          b.project(
              input ->
                  keep.stream()
                      .map(k -> (Expression) b.fieldReference(input, k))
                      .collect(Collectors.toList()),
              // Project appends expressions after the input columns; emit only the
              // appended ones so the output is exactly the kept columns.
              Rel.Remap.offset(inputWidth, keep.size()),
              rel);
      outputNames = projection;
    }

    if (limit.isPresent()) {
      rel = b.limit(limit.getAsLong(), rel);
    }

    Plan plan = b.plan(Plan.Root.builder().input(rel).addAllNames(outputNames).build());
    return new PlanProtoConverter().toProto(plan).toByteArray();
  }

  // --- Spark Filter -> Substrait Expression ---------------------------------

  /** Whether {@code f} maps to a standard-catalog Substrait predicate over known columns. */
  static boolean canPush(Filter f, Set<String> columns) {
    if (f instanceof EqualTo e) {
      return columns.contains(e.attribute()) && isPushableLiteral(e.value());
    } else if (f instanceof GreaterThan e) {
      return columns.contains(e.attribute()) && isPushableLiteral(e.value());
    } else if (f instanceof GreaterThanOrEqual e) {
      return columns.contains(e.attribute()) && isPushableLiteral(e.value());
    } else if (f instanceof LessThan e) {
      return columns.contains(e.attribute()) && isPushableLiteral(e.value());
    } else if (f instanceof LessThanOrEqual e) {
      return columns.contains(e.attribute()) && isPushableLiteral(e.value());
    } else if (f instanceof IsNull e) {
      return columns.contains(e.attribute());
    } else if (f instanceof IsNotNull e) {
      return columns.contains(e.attribute());
    } else if (f instanceof And e) {
      return canPush(e.left(), columns) && canPush(e.right(), columns);
    } else if (f instanceof Or e) {
      return canPush(e.left(), columns) && canPush(e.right(), columns);
    } else if (f instanceof Not e) {
      return canPush(e.child(), columns);
    }
    return false;
  }

  private static Expression conjunction(
      SubstraitBuilder b, Rel input, Map<String, Integer> idx, List<Filter> filters) {
    List<Expression> terms =
        filters.stream().map(f -> translate(b, input, idx, f)).collect(Collectors.toList());
    if (terms.size() == 1) {
      return terms.get(0);
    }
    return b.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_BOOLEAN,
        "and:bool",
        BOOL,
        terms.toArray(new Expression[0]));
  }

  private static Expression translate(
      SubstraitBuilder b, Rel input, Map<String, Integer> idx, Filter f) {
    if (f instanceof EqualTo e) {
      return cmp(b, input, idx, "equal:any_any", e.attribute(), e.value());
    } else if (f instanceof GreaterThan e) {
      return cmp(b, input, idx, "gt:any_any", e.attribute(), e.value());
    } else if (f instanceof GreaterThanOrEqual e) {
      return cmp(b, input, idx, "gte:any_any", e.attribute(), e.value());
    } else if (f instanceof LessThan e) {
      return cmp(b, input, idx, "lt:any_any", e.attribute(), e.value());
    } else if (f instanceof LessThanOrEqual e) {
      return cmp(b, input, idx, "lte:any_any", e.attribute(), e.value());
    } else if (f instanceof IsNull e) {
      return b.scalarFn(
          DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
          "is_null:any",
          BOOL,
          b.fieldReference(input, idx.get(e.attribute())));
    } else if (f instanceof IsNotNull e) {
      return b.scalarFn(
          DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
          "is_not_null:any",
          BOOL,
          b.fieldReference(input, idx.get(e.attribute())));
    } else if (f instanceof And e) {
      return b.scalarFn(
          DefaultExtensionCatalog.FUNCTIONS_BOOLEAN,
          "and:bool",
          BOOL,
          translate(b, input, idx, e.left()),
          translate(b, input, idx, e.right()));
    } else if (f instanceof Or e) {
      return b.scalarFn(
          DefaultExtensionCatalog.FUNCTIONS_BOOLEAN,
          "or:bool",
          BOOL,
          translate(b, input, idx, e.left()),
          translate(b, input, idx, e.right()));
    } else if (f instanceof Not e) {
      return b.scalarFn(
          DefaultExtensionCatalog.FUNCTIONS_BOOLEAN,
          "not:bool",
          BOOL,
          translate(b, input, idx, e.child()));
    }
    throw new IllegalArgumentException("filter is not pushable: " + f);
  }

  private static Expression cmp(
      SubstraitBuilder b,
      Rel input,
      Map<String, Integer> idx,
      String key,
      String attribute,
      Object value) {
    return b.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
        key,
        BOOL,
        b.fieldReference(input, idx.get(attribute)),
        literal(value));
  }

  private static boolean isPushableLiteral(Object v) {
    // Exclude non-finite floats: they have no SQL literal, and the connector may
    // encode this predicate as SQL (when the driver lacks Substrait support), so
    // the pushable set must be expressible in both wires.
    if (v instanceof Double d) {
      return !d.isNaN() && !d.isInfinite();
    }
    if (v instanceof Float f) {
      return !f.isNaN() && !f.isInfinite();
    }
    return v instanceof Integer
        || v instanceof Long
        || v instanceof Short
        || v instanceof Byte
        || v instanceof Boolean
        || v instanceof String;
  }

  private static Expression literal(Object v) {
    if (v instanceof Integer n) {
      return ExpressionCreator.i32(true, n);
    } else if (v instanceof Long n) {
      return ExpressionCreator.i64(true, n);
    } else if (v instanceof Short n) {
      return ExpressionCreator.i16(true, n);
    } else if (v instanceof Byte n) {
      return ExpressionCreator.i8(true, n);
    } else if (v instanceof Double n) {
      return ExpressionCreator.fp64(true, n);
    } else if (v instanceof Float n) {
      return ExpressionCreator.fp32(true, n);
    } else if (v instanceof Boolean n) {
      return ExpressionCreator.bool(true, n);
    } else if (v instanceof String n) {
      return ExpressionCreator.string(true, n);
    }
    throw new IllegalArgumentException("unsupported literal type: " + v.getClass());
  }

  // --- Arrow type -> Substrait type -----------------------------------------

  private static Type toSubstraitType(Field field) {
    ArrowType type = field.getType();
    TypeCreator t = field.isNullable() ? TypeCreator.NULLABLE : TypeCreator.REQUIRED;
    if (type instanceof ArrowType.Int n) {
      if (!n.getIsSigned()) {
        throw unsupported(field);
      }
      return switch (n.getBitWidth()) {
        case 8 -> t.I8;
        case 16 -> t.I16;
        case 32 -> t.I32;
        case 64 -> t.I64;
        default -> throw unsupported(field);
      };
    }
    if (type instanceof ArrowType.FloatingPoint fp) {
      return fp.getPrecision() == FloatingPointPrecision.DOUBLE ? t.FP64 : t.FP32;
    }
    if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
      return t.STRING;
    }
    if (type instanceof ArrowType.Bool) {
      return t.BOOLEAN;
    }
    throw unsupported(field);
  }

  private static IllegalArgumentException unsupported(Field field) {
    return new IllegalArgumentException(
        "unsupported Arrow type for column '" + field.getName() + "': " + field.getType());
  }
}
