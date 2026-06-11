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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}

/**
 * Marker for the connector's task payloads, shipped driver → executor via Java serialization.
 * [[DatafusionPartitionReaderFactory]] dispatches on the concrete type.
 */
sealed trait DatafusionPartition extends InputPartition

/**
 * Per-task payload for the per-partition payload (legacy) read path.
 *
 *  - `factoryFqcn`: fully-qualified class name of the bridge's `BridgeProviderFactory`. The
 *    executor reflectively instantiates this and calls
 *    `scanBackend().createScan(optionsBytes, partitionBytes, …)`.
 *  - `optionsBytes`: bridge-specific global connection options, encoded by the bridge.
 *    Opaque to connector-core. Same bytes ride along on every partition.
 *  - `projectionColumnNames`: pruned column list (post-`pruneColumns`).
 *  - `filterProtoBytes`: V2 `Predicate` → DataFusion `LogicalExprNode` proto bytes; each one is
 *    applied natively via `ScanBackend.createScan`.
 *  - `partitionId`: stable identifier (e.g. a segment or file id) — surfaces in Spark UI/logs/errors.
 *  - `partitionBytes`: opaque per-partition payload from `PartitionInfo.partitionBytes`. Passed
 *    back into `ScanBackend.createScan` so the bridge materialises *this* slice.
 *  - `preferredLocs`: hostnames where this partition's data lives; returned from
 *    `preferredLocations()` so Spark schedules the task there subject to `spark.locality.wait`.
 */
final case class DatafusionInputPartition(
    factoryFqcn: String,
    optionsBytes: Array[Byte],
    projectionColumnNames: Array[String],
    filterProtoBytes: Array[Array[Byte]],
    partitionId: String,
    partitionBytes: Array[Byte],
    preferredLocs: Array[String]
) extends DatafusionPartition {

  override def preferredLocations(): Array[String] = preferredLocs
}

/**
 * Legacy-path payload that additionally carries this partition's key values, precomputed
 * driver-side into an [[InternalRow]]. Emitted by [[DatafusionBatch]] when the bridge reported a
 * partitioning AND every `PartitionInfo` carries `partitionKeyValues` — implementing
 * [[HasPartitionKey]] is what makes the reported `KeyGroupedPartitioning` visible to Spark 3.3+
 * (`DataSourceV2ScanExecBase.groupPartitions` ignores it otherwise).
 */
final case class DatafusionKeyedInputPartition(
    base: DatafusionInputPartition,
    keyRow: InternalRow
) extends DatafusionPartition
    with HasPartitionKey {

  override def preferredLocations(): Array[String] = base.preferredLocations()

  override def partitionKey(): InternalRow = keyRow
}

/**
 * Per-task payload for shared-scan mode: task `partitionIndex` streams that DataFusion plan
 * partition from the executor's cached entry (see [[SharedScanCache]]).
 *
 *  - `scanId`: driver-minted UUID identifying this scan; the executor cache key.
 *  - `partitionIndex`: DataFusion output partition this task drives.
 *  - `numPartitions`: the driver probe's partition count; executors fail fast when their re-plan
 *    diverges (determinism guard).
 *  - `pinnedConfig`: DataFusion session knobs resolved once on the driver and replicated on
 *    every executor so both plan identically.
 *  - `idleTtlMs`: cache-entry idle eviction window, resolved from driver conf.
 *
 * No preferred locations: the shared plan materialises the whole dataset on whichever executors
 * Spark picks; there is no per-slice host mapping in this mode.
 */
final case class DatafusionSharedScanPartition(
    factoryFqcn: String,
    optionsBytes: Array[Byte],
    projectionColumnNames: Array[String],
    filterProtoBytes: Array[Array[Byte]],
    scanId: String,
    partitionIndex: Int,
    numPartitions: Int,
    pinnedConfig: PinnedSessionConfig,
    idleTtlMs: Long
) extends DatafusionPartition {

  def toSpec: SharedScanSpec =
    SharedScanSpec(
      scanId = scanId,
      factoryFqcn = factoryFqcn,
      optionsBytes = optionsBytes,
      projectionColumnNames = projectionColumnNames,
      filterProtoBytes = filterProtoBytes,
      pinnedConfig = pinnedConfig
    )
}
