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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Builds an {@link AdbcScanImpl}, pushing projection / filter / limit down as Substrait.
 *
 * <p>Pushdown <em>decision</em>: a filter is pushed only if {@link SubstraitPlan#canPush} maps it
 * to a standard-catalog Substrait predicate over a known column; the rest are returned to Spark.
 * Projection becomes a Substrait emit. Limit is pushed only when no filters were left to Spark --
 * otherwise Spark must still filter after the scan, so a scan-side limit would be wrong.
 */
final class AdbcScanBuilder
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit {

  private final StructType fullSchema;
  private final AdbcOptions options;

  private StructType requiredSchema;
  private List<Filter> pushedFilters = new ArrayList<>();
  private boolean hasResidualFilters = false;
  private OptionalLong limit = OptionalLong.empty();

  AdbcScanBuilder(StructType schema, AdbcOptions options) {
    this.fullSchema = schema;
    this.requiredSchema = schema;
    this.options = options;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    // Push filters only against Spark-native (non-cast) columns: a cast column is cast in the
    // scan projection, but a pushed predicate runs against the pre-cast source column, so its
    // Spark-domain literal would not match. Those filters are left to Spark (see SchemaConverter).
    Set<String> columns = new LinkedHashSet<>(Arrays.asList(fullSchema.fieldNames()));
    for (StructField field : fullSchema.fields()) {
      if (field.metadata().contains(SchemaConverter.CAST_METADATA_KEY)) {
        columns.remove(field.name());
      }
    }
    List<Filter> pushable = new ArrayList<>();
    List<Filter> residual = new ArrayList<>();
    for (Filter f : filters) {
      if (SubstraitPlan.canPush(f, columns)) {
        pushable.add(f);
      } else {
        residual.add(f);
      }
    }
    this.pushedFilters = pushable;
    this.hasResidualFilters = !residual.isEmpty();
    return residual.toArray(new Filter[0]);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters.toArray(new Filter[0]);
  }

  @Override
  public boolean pushLimit(int limit) {
    // Only safe to push when Spark has no filters left to apply after the scan;
    // otherwise the limit would be applied before that filtering.
    if (hasResidualFilters) {
      return false;
    }
    this.limit = OptionalLong.of(limit);
    return true;
  }

  @Override
  public Scan build() {
    List<String> projection =
        Arrays.equals(requiredSchema.fieldNames(), fullSchema.fieldNames())
            ? null
            : Arrays.asList(requiredSchema.fieldNames());
    return new AdbcScanImpl(requiredSchema, options, projection, pushedFilters, limit);
  }
}
