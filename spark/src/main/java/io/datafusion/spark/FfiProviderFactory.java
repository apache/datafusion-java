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

import java.util.Map;

/**
 * Bridge interface implemented per domain (HDF5, custom Iceberg, an in-house format, etc.). A bridge owns its
 * own proto schema for connection options and a cdylib that produces an {@code FFI_TableProvider}
 * pointer. The connector-core Spark plumbing is generic — it knows only this interface.
 *
 * <p>Lifecycle per Spark task:
 *
 * <ol>
 *   <li>{@link #encodeOptions(Map)} — driver-side, converts the Spark options map into the bridge's
 *       own proto bytes; ships verbatim through {@code DatafusionInputPartition}.
 *   <li>{@link #listPartitions(byte[])} — driver-side, enumerates partitions as {@link
 *       PartitionInfo} entries. One Spark task is created per entry. Each entry carries an opaque
 *       {@code partitionBytes} payload that is shipped to the executor and replayed into {@link
 *       #createProvider(byte[], byte[])}, plus optional {@code preferredLocations} hostnames that
 *       drive Spark's data-locality scheduling.
 *   <li>{@link #createProvider(byte[], byte[])} — executor-side, builds the bridge's {@code
 *       Arc&lt;dyn TableProvider&gt;} for this specific partition, wraps it in an {@code
 *       FFI_TableProvider}, returns the raw boxed pointer as a {@code jlong}. The caller owns this
 *       pointer and is responsible for handing it to exactly one consumer (the consumer's {@code
 *       Drop} releases it).
 * </ol>
 *
 * <p>Implementations must be no-arg constructable so the Spark connector can instantiate them
 * reflectively via {@link Class#forName(String)} on the executor.
 */
public interface FfiProviderFactory {

  /**
   * Convert Spark's flat option map to the bridge's proto-encoded options. Driver-side only.
   *
   * @throws IllegalArgumentException if required options are missing or invalid
   */
  byte[] encodeOptions(Map<String, String> sparkOptions);

  /**
   * Enumerate partitions for this dataset. One Spark task is created per returned {@link
   * PartitionInfo}. Driver-side only.
   *
   * <p>Each partition's {@code partitionBytes} ships verbatim through {@code
   * DatafusionInputPartition} to the executor, where it is passed to {@link #createProvider(byte[],
   * byte[])}. Use it to encode whatever slice metadata (row range, sub-options, file offsets,
   * segment id, …) the bridge needs to materialise *that* partition.
   *
   * <p>Each partition's {@code preferredLocations} hostnames are returned from {@code
   * InputPartition.preferredLocations()} so Spark co-locates the task with the data; empty array =
   * no preference.
   */
  PartitionInfo[] listPartitions(byte[] optionsProtoBytes);

  /**
   * Filter-aware variant of {@link #listPartitions(byte[])}. The connector calls this overload with
   * the pushed-down predicates ({@code LogicalExprNode} proto bytes, one array per predicate, same
   * encoding the executor later replays via {@code FfiHelperNative.createScan}). Bridges that can
   * map predicates onto their partition layout (e.g. {@code segment_id = 'x'}) should prune
   * partitions that cannot match — pruning here eliminates whole Spark tasks, whereas the per-task
   * filter only reduces rows inside a task.
   *
   * <p>Pruning must be conservative: only drop a partition when NO row in it can satisfy the
   * conjunction of all pushed predicates. The default delegates to the filter-unaware overload (no
   * pruning), which is always correct.
   */
  default PartitionInfo[] listPartitions(byte[] optionsProtoBytes, byte[][] filterProtoBytes) {
    return listPartitions(optionsProtoBytes);
  }

  /**
   * Opt into shared-scan mode for this dataset. Default {@code false} (per-partition payload mode,
   * the {@link #listPartitions(byte[])} path).
   *
   * <p>When {@code true}, the connector builds ONE provider per (executor JVM × scan) with empty
   * {@code partitionBytes}, plans it once, and runs one Spark task per DataFusion output partition
   * — task {@code i} streams plan partition {@code i} from the shared, cached plan. This amortises
   * {@code createProvider} cost across all tasks on an executor and is the right model when the
   * dataset has many small partitions or provider construction is expensive (remote metadata,
   * connections). {@link #listPartitions(byte[])} and {@link #reportPartitioning(byte[])} are NOT
   * called in this mode, and the scan reports {@code UnknownPartitioning} (DataFusion-native
   * partitions carry no key contract).
   *
   * <p><b>Determinism contract.</b> The driver counts partitions by planning once; every executor
   * re-plans independently and must arrive at the same result. A bridge returning {@code true}
   * guarantees:
   *
   * <ul>
   *   <li>The provider's schema, partitioning, and per-partition row content are a pure function of
   *       {@code optionsProtoBytes}. Remote sources must pin a snapshot (version, timestamp) inside
   *       the options; data that compacts or moves between driver planning and executor execution
   *       otherwise yields wrong results that no runtime check can catch.
   *   <li>The provider's {@code ExecutionPlan} supports calling {@code execute(i)} more than once
   *       per plan instance (Spark task retry and speculative execution re-execute a partition
   *       index, sometimes concurrently). Stateless scans satisfy this; single-shot streams do not.
   * </ul>
   *
   * <p>The connector fails tasks with a clear error when the executor's partition count diverges
   * from the driver's — but identical counts with different contents cannot be detected.
   */
  default boolean sharedScan(byte[] optionsProtoBytes) {
    return false;
  }

  /**
   * Build the underlying {@code Arc<dyn TableProvider>} for one partition and wrap it in an {@code
   * FFI_TableProvider}. Returns the raw {@code Box::into_raw} pointer as a {@code jlong}; the
   * caller takes ownership.
   *
   * @param optionsProtoBytes global options produced by {@link #encodeOptions(Map)}
   * @param partitionBytes per-partition slice payload from {@link PartitionInfo#partitionBytes()}.
   *     Empty array for single-partition tables and for the driver-side schema probe in {@code
   *     DatafusionSource.inferSchema}.
   */
  long createProvider(byte[] optionsProtoBytes, byte[] partitionBytes);

  /**
   * Declare how rows are partitioned across the {@link PartitionInfo} entries returned by {@link
   * #listPartitions(byte[])}. Driver-side only.
   *
   * <p>When non-null, the connector surfaces a {@code KeyGroupedPartitioning(keys,
   * listPartitions(...).length)} to Spark via {@code SupportsReportPartitioning} so the optimizer
   * can elide shuffles ahead of joins/aggregations on the declared keys.
   *
   * <p>Default returns {@code null} — no partitioning guarantees, Spark plans as if the scan's
   * output ordering and grouping are unknown.
   *
   * <p>If a bridge implements this, it must hold the {@link ReportedPartitioning} contract: every
   * row in a given partition evaluates to the same tuple of key values under the declared
   * transforms.
   *
   * <p><b>Spark 3.3+ caveat:</b> the reported partitioning only takes effect when every {@link
   * PartitionInfo} also carries {@link PartitionInfo#partitionKeyValues()} (surfaced to Spark via
   * {@code HasPartitionKey}); without key values Spark ignores the declared {@code
   * KeyGroupedPartitioning} entirely. Storage-partitioned joins additionally require {@code
   * spark.sql.sources.v2.bucketing.enabled=true}.
   */
  default ReportedPartitioning reportPartitioning(byte[] optionsProtoBytes) {
    return null;
  }
}
