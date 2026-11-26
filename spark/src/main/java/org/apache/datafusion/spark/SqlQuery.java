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

import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.apache.datafusion.spark.SchemaConverter.ProjectionColumn;
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

/**
 * Renders the pushed-down scan as an ANSI SQL {@code SELECT}.
 *
 * <p>This is the fallback wire for ADBC clients that don't accept Substrait. The arrow-adbc JNI
 * driver manager (0.23) forwards {@code SetSqlQuery} but not {@code SetSubstraitPlan}, so the
 * connector prefers Substrait and falls back to this. The pushable predicate set is identical to
 * {@link SubstraitPlan} (so the {@link AdbcScanBuilder} decision is encoding-independent) -- which
 * is why {@link SubstraitPlan#canPush} excludes non-finite floats: they have no SQL literal.
 *
 * <p>Identifiers are double-quoted (ANSI, DataFusion's default) and string literals single-quoted,
 * both with doubling-based escaping.
 *
 * <p>Columns whose source Arrow type is not Spark-native (see {@link SchemaConverter#needsCast})
 * are wrapped in {@code arrow_cast(col, '<arrow type>')} and re-aliased to their original name, so
 * the scan emits Spark-native Arrow and the output column names still match the reported schema.
 */
final class SqlQuery {

  private SqlQuery() {}

  static String build(
      String table, List<ProjectionColumn> columns, List<Filter> filters, OptionalLong limit) {
    StringBuilder sql = new StringBuilder("SELECT ");
    if (columns == null || columns.isEmpty()) {
      sql.append("*");
    } else {
      sql.append(columns.stream().map(SqlQuery::column).collect(Collectors.joining(", ")));
    }
    sql.append(" FROM ").append(quoteId(table));
    if (!filters.isEmpty()) {
      sql.append(" WHERE ")
          .append(filters.stream().map(SqlQuery::predicate).collect(Collectors.joining(" AND ")));
    }
    if (limit.isPresent()) {
      sql.append(" LIMIT ").append(limit.getAsLong());
    }
    return sql.toString();
  }

  private static String column(ProjectionColumn c) {
    if (c.castType() == null) {
      return quoteId(c.name());
    }
    // arrow_cast renames its output, so alias back to the source name.
    return "arrow_cast(" + quoteId(c.name()) + ", '" + c.castType() + "') AS " + quoteId(c.name());
  }

  private static String predicate(Filter f) {
    if (f instanceof EqualTo e) {
      return binary(e.attribute(), "=", e.value());
    } else if (f instanceof GreaterThan e) {
      return binary(e.attribute(), ">", e.value());
    } else if (f instanceof GreaterThanOrEqual e) {
      return binary(e.attribute(), ">=", e.value());
    } else if (f instanceof LessThan e) {
      return binary(e.attribute(), "<", e.value());
    } else if (f instanceof LessThanOrEqual e) {
      return binary(e.attribute(), "<=", e.value());
    } else if (f instanceof IsNull e) {
      return "(" + quoteId(e.attribute()) + " IS NULL)";
    } else if (f instanceof IsNotNull e) {
      return "(" + quoteId(e.attribute()) + " IS NOT NULL)";
    } else if (f instanceof And e) {
      return "(" + predicate(e.left()) + " AND " + predicate(e.right()) + ")";
    } else if (f instanceof Or e) {
      return "(" + predicate(e.left()) + " OR " + predicate(e.right()) + ")";
    } else if (f instanceof Not e) {
      return "(NOT " + predicate(e.child()) + ")";
    }
    throw new IllegalArgumentException("filter is not pushable to SQL: " + f);
  }

  private static String binary(String attribute, String op, Object value) {
    return "(" + quoteId(attribute) + " " + op + " " + literal(value) + ")";
  }

  private static String literal(Object v) {
    if (v instanceof Boolean b) {
      return b ? "TRUE" : "FALSE";
    } else if (v instanceof String s) {
      return "'" + s.replace("'", "''") + "'";
    } else if (v instanceof Integer
        || v instanceof Long
        || v instanceof Short
        || v instanceof Byte) {
      return v.toString();
    } else if (v instanceof Double || v instanceof Float) {
      // SubstraitPlan.canPush has already rejected NaN/Infinity, so toString is a
      // valid SQL numeric literal here.
      return v.toString();
    }
    throw new IllegalArgumentException("unsupported SQL literal type: " + v.getClass());
  }

  private static String quoteId(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
