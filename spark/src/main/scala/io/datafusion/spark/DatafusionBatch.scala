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

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

/**
 * Spark `Batch` for a DataFusion-backed scan. Driver-side partition planning:
 *   - [[LegacyMode]]: one task per `PartitionInfo` (resolved by [[DatafusionScanBuilder]]); when
 *     the bridge reported a partitioning and every entry carries key values, tasks implement
 *     `HasPartitionKey` so Spark can actually use the `KeyGroupedPartitioning`.
 *   - [[SharedScanMode]]: one task per DataFusion plan partition index.
 */
class DatafusionBatch(val scan: DatafusionScan) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    val projection = scan.prunedSchema.fieldNames
    val filterBytes: Array[Array[Byte]] = scan.pushedPredicateBytes

    scan.mode match {
      case LegacyMode(partitions, reported) =>
        val keyed = DatafusionBatch.validateKeyedState(scan.factoryFqcn, partitions, reported)
        partitions.iterator.map { p =>
          val base = DatafusionInputPartition(
            factoryFqcn = scan.factoryFqcn,
            optionsBytes = scan.optionsBytes,
            projectionColumnNames = projection,
            filterProtoBytes = filterBytes,
            partitionId = p.id,
            partitionBytes = p.partitionBytes,
            preferredLocs = p.preferredLocations
          )
          val out: DatafusionPartition =
            if (keyed) {
              DatafusionKeyedInputPartition(
                base,
                DatafusionBatch.toKeyRow(p.id, p.partitionKeyValues, reported))
            } else base
          out.asInstanceOf[InputPartition]
        }.toArray

      case SharedScanMode(scanId, numPartitions, pinnedConfig, idleTtlMs) =>
        Array.tabulate[InputPartition](numPartitions) { i =>
          DatafusionSharedScanPartition(
            factoryFqcn = scan.factoryFqcn,
            optionsBytes = scan.optionsBytes,
            projectionColumnNames = projection,
            filterProtoBytes = filterBytes,
            scanId = scanId,
            partitionIndex = i,
            numPartitions = numPartitions,
            pinnedConfig = pinnedConfig,
            idleTtlMs = idleTtlMs
          )
        }
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new DatafusionPartitionReaderFactory(scan.prunedSchema)
}

private[spark] object DatafusionBatch {

  /**
   * Keyed partitions require a reported partitioning AND key values on EVERY partition. A mixed
   * state means the bridge violated its own contract; failing driver-side beats Spark silently
   * planning without the declared grouping.
   */
  def validateKeyedState(
      factoryFqcn: String,
      partitions: Array[PartitionInfo],
      reported: ReportedPartitioning): Boolean = {
    if (reported == null) {
      return false
    }
    val withKeys = partitions.count(_.partitionKeyValues != null)
    if (withKeys == 0) {
      return false
    }
    if (withKeys != partitions.length) {
      throw new IllegalStateException(
        s"BridgeProviderFactory '$factoryFqcn' reported a partitioning but only $withKeys of " +
          s"${partitions.length} PartitionInfo entries carry partitionKeyValues; either all " +
          "partitions must carry key values or none")
    }
    true
  }

  /**
   * Convert a bridge-supplied `Object[]` of key values into Spark's internal row representation
   * (String → UTF8String, Instant → micros, LocalDate → days, BigDecimal → Decimal, ...).
   */
  def toKeyRow(
      partitionId: String,
      values: Array[AnyRef],
      reported: ReportedPartitioning): InternalRow = {
    val keyCount = reported.keys().length
    if (values.length != keyCount) {
      throw new IllegalStateException(
        s"PartitionInfo '$partitionId' carries ${values.length} partitionKeyValues but the " +
          s"reported partitioning declares $keyCount key(s)")
    }
    val converted = values.map(v => CatalystTypeConverters.convertToCatalyst(v))
    new GenericInternalRow(converted)
  }
}
