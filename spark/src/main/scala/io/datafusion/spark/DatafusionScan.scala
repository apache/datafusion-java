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
 * Read plan for a DataFusion-backed scan. Holds pruning state, the pushed predicates (for
 * `description()` / `explain(True)`), and the corresponding `LogicalExprNode` proto byte arrays
 * the executor applies via `DataFrame.filterFromProto`.
 *
 * Also carries the driver-resolved `PartitionInfo[]` (so [[DatafusionBatch]] doesn't re-call
 * `listPartitions`) and the optional bridge-declared [[ReportedPartitioning]]; when present, the
 * scan surfaces a `KeyGroupedPartitioning` via `SupportsReportPartitioning` so Spark's optimizer
 * can skip shuffles ahead of compatible joins/aggregations. When absent, an
 * `UnknownPartitioning(partitions.length)` is reported (still correct, just no shuffle elision).
 */
class DatafusionScan(
    val factoryFqcn: String,
    val optionsProtoBytes: Array[Byte],
    val fullSchema: StructType,
    val prunedSchema: StructType,
    val pushedPredicates: Array[Predicate],
    val pushedPredicateBytes: Array[Array[Byte]],
    val partitions: Array[PartitionInfo],
    val reportedPartitioning: ReportedPartitioning
) extends Scan
    with SupportsReportPartitioning {

  override def readSchema(): StructType = prunedSchema

  override def description(): String =
    s"DatafusionScan(factory=$factoryFqcn, projection=${prunedSchema.fieldNames.mkString(",")}," +
      s" pushedPredicates=${pushedPredicates.length}, partitions=${partitions.length}," +
      s" reportedPartitioning=${if (reportedPartitioning == null) "unknown" else "key-grouped"})"

  override def toBatch: Batch = new DatafusionBatch(this)

  override def outputPartitioning(): Partitioning =
    if (reportedPartitioning == null) new UnknownPartitioning(partitions.length)
    else new KeyGroupedPartitioning(reportedPartitioning.keys().toArray, partitions.length)
}
