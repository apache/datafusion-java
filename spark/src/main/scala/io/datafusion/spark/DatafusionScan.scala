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

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, Scan, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{
  KeyGroupedPartitioning,
  Partitioning,
  UnknownPartitioning
}
import org.apache.spark.sql.types.StructType

/**
 * How the scan maps to Spark tasks — resolved once, driver-side, in
 * [[DatafusionScanBuilder.build]].
 */
sealed trait DatafusionScanMode extends Serializable

/**
 * Per-partition payload mode: one task per [[PartitionInfo]], each task builds its own provider
 * from that entry's `partitionBytes`. `reported` is the bridge's optional partitioning
 * declaration (may be null).
 */
final case class LegacyMode(
    partitions: Array[PartitionInfo],
    reported: ReportedPartitioning
) extends DatafusionScanMode

/**
 * Shared-scan mode: one cached provider + plan per (executor × scan), `numPartitions` tasks each
 * driving one DataFusion output partition. See [[BridgeProviderFactory#sharedScan]] for the
 * determinism contract.
 */
final case class SharedScanMode(
    scanId: String,
    numPartitions: Int,
    pinnedConfig: PinnedSessionConfig,
    idleTtlMs: Long
) extends DatafusionScanMode

/**
 * Read plan for a DataFusion-backed scan. Holds pruning state, the pushed predicates (for
 * `description()` / `explain(True)`), the corresponding `LogicalExprNode` proto byte arrays the
 * executor applies natively via `ScanBackend.createScan`, and the driver-resolved
 * [[DatafusionScanMode]].
 *
 * Legacy mode with a bridge-declared [[ReportedPartitioning]] surfaces `KeyGroupedPartitioning`
 * via `SupportsReportPartitioning`; note Spark 3.3+ only consumes it when the input partitions
 * also implement `HasPartitionKey` (see [[DatafusionBatch]]). Shared-scan mode always reports
 * `UnknownPartitioning` — DataFusion-native partitions carry no key contract.
 */
class DatafusionScan(
    val factoryFqcn: String,
    val optionsBytes: Array[Byte],
    val fullSchema: StructType,
    val prunedSchema: StructType,
    val pushedPredicates: Array[Predicate],
    val pushedPredicateBytes: Array[Array[Byte]],
    val mode: DatafusionScanMode
) extends Scan
    with SupportsReportPartitioning {

  override def readSchema(): StructType = prunedSchema

  override def description(): String = {
    val modeDesc = mode match {
      case LegacyMode(partitions, reported) =>
        s"mode=per-partition, partitions=${partitions.length}," +
          s" reportedPartitioning=${if (reported == null) "unknown" else "key-grouped"}"
      case SharedScanMode(scanId, n, _, _) =>
        s"mode=shared-scan, scanId=$scanId, partitions=$n"
    }
    s"DatafusionScan(factory=$factoryFqcn, projection=${prunedSchema.fieldNames.mkString(",")}," +
      s" pushedPredicates=${pushedPredicates.length}, $modeDesc)"
  }

  override def toBatch: Batch = new DatafusionBatch(this)

  override def outputPartitioning(): Partitioning = mode match {
    case LegacyMode(partitions, reported) =>
      if (reported == null) new UnknownPartitioning(partitions.length)
      else new KeyGroupedPartitioning(reported.keys().toArray, partitions.length)
    case SharedScanMode(_, numPartitions, _, _) =>
      new UnknownPartitioning(numPartitions)
  }
}
