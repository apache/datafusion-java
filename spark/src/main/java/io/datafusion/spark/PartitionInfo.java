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

/**
 * Driver-side descriptor for a single partition produced by {@link
 * FfiProviderFactory#listPartitions(byte[])}. Carries the bridge-specific slice payload that the
 * executor passes back into {@link FfiProviderFactory#createProvider(byte[], byte[])}, plus
 * optional host hints for Spark's scheduler.
 *
 * <p>Fields:
 *
 * <ul>
 *   <li>{@code id} — stable, human-readable identifier for this partition (e.g. a Rerun segment
 *       id). Surfaces in Spark UI, logs, and exception messages. Must be non-empty.
 *   <li>{@code partitionBytes} — opaque per-partition payload. Bridge encodes whatever the executor
 *       needs to materialise *this* slice (offsets, row ranges, sub-options, etc.). Combined with
 *       the global {@code optionsProtoBytes} in {@link FfiProviderFactory#createProvider(byte[],
 *       byte[])}. Empty array = no per-partition state (single-partition table).
 *   <li>{@code preferredLocations} — hostnames where this partition's data lives. Returned from
 *       {@code InputPartition.preferredLocations()} so Spark can co-locate the task with the data.
 *       Empty array = no preference. Honoured subject to {@code spark.locality.wait}.
 *   <li>{@code partitionKeyValues} — optional values of the partitioning keys for every row in this
 *       partition, in the same order as {@link FfiProviderFactory#reportPartitioning(byte[])}'s
 *       declared transforms. {@code null} = no key (the default). When the bridge reports a
 *       partitioning AND every partition carries key values, the connector exposes them to Spark
 *       via {@code HasPartitionKey} — required on Spark 3.3+ for the reported {@code
 *       KeyGroupedPartitioning} to have any effect (and storage-partitioned joins additionally
 *       require {@code spark.sql.sources.v2.bucketing.enabled=true}). Values must be Java types
 *       that Spark's {@code CatalystTypeConverters} can convert for the key columns' data types
 *       (e.g. {@code String}, {@code Long}, {@code Integer}, {@code java.time.Instant}, {@code
 *       java.time.LocalDate}, {@code java.math.BigDecimal}), and the array length must equal the
 *       number of declared keys.
 * </ul>
 */
public record PartitionInfo(
    String id, byte[] partitionBytes, String[] preferredLocations, Object[] partitionKeyValues) {

  public PartitionInfo {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("PartitionInfo: id must be non-empty");
    }
    if (partitionBytes == null) {
      partitionBytes = new byte[0];
    }
    if (preferredLocations == null) {
      preferredLocations = new String[0];
    }
    // partitionKeyValues stays null when absent: null and "no key" are the same state,
    // and DatafusionBatch distinguishes keyed from unkeyed partitions by it.
  }

  /** Without partition key values — the common case. */
  public PartitionInfo(String id, byte[] partitionBytes, String[] preferredLocations) {
    this(id, partitionBytes, preferredLocations, null);
  }
}
