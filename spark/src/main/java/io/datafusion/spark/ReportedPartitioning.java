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

package io.datafusion.spark;

import java.util.Arrays;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;

/**
 * Driver-side declaration of how a bridge's data is partitioned on the key columns. When supplied
 * via {@link BridgeProviderFactory#reportPartitioning(byte[])}, the connector surfaces a {@link
 * org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning} from {@link
 * org.apache.spark.sql.connector.read.SupportsReportPartitioning#outputPartitioning()} — Spark's
 * optimizer can then skip the shuffle ahead of joins/aggregations whose grouping keys line up with
 * these transforms.
 *
 * <p>Contract: for any partition reported by {@link BridgeProviderFactory#listPartitions(byte[])},
 * every row produced by that partition must evaluate to the same tuple of key values under these
 * transforms. Different partitions <i>may</i> share key values (Spark will fuse them); they <b>must
 * not</b> straddle key values.
 *
 * <p>The partition count Spark sees is {@code listPartitions(...).length}; it is not carried here
 * to keep a single source of truth.
 */
public final class ReportedPartitioning {

  private final Transform[] keys;

  public ReportedPartitioning(Transform[] keys) {
    if (keys == null || keys.length == 0) {
      throw new IllegalArgumentException(
          "ReportedPartitioning: keys must contain at least one transform");
    }
    this.keys = keys;
  }

  public Transform[] keys() {
    return keys;
  }

  /**
   * Convenience: declare identity partitioning on one or more columns (a row in partition P has the
   * same {@code (col1, col2, …)} values as every other row in P).
   */
  public static ReportedPartitioning identity(String... columns) {
    if (columns == null || columns.length == 0) {
      throw new IllegalArgumentException(
          "ReportedPartitioning.identity: at least one column required");
    }
    Transform[] ts = Arrays.stream(columns).map(Expressions::identity).toArray(Transform[]::new);
    return new ReportedPartitioning(ts);
  }

  /**
   * Convenience: declare hash-bucket partitioning. Mirrors Spark's {@code bucket(N, cols…)}
   * transform — each row is assigned to bucket {@code hash(cols) mod numBuckets}.
   */
  public static ReportedPartitioning bucket(int numBuckets, String... columns) {
    if (numBuckets <= 0) {
      throw new IllegalArgumentException(
          "ReportedPartitioning.bucket: numBuckets must be > 0, got " + numBuckets);
    }
    if (columns == null || columns.length == 0) {
      throw new IllegalArgumentException(
          "ReportedPartitioning.bucket: at least one column required");
    }
    return new ReportedPartitioning(new Transform[] {Expressions.bucket(numBuckets, columns)});
  }
}
