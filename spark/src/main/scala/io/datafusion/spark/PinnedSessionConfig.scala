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

import org.apache.spark.sql.internal.SQLConf

/**
 * DataFusion session knobs pinned by the driver and replicated verbatim on every executor in
 * shared-scan mode.
 *
 * DataFusion's default `SessionConfig` derives `target_partitions` from the machine's core count,
 * so a plan that yields N partitions on the driver could yield M ≠ N on a differently-sized
 * executor — and partition-indexed execution would silently drop or duplicate data. The driver
 * resolves these values once in `DatafusionScanBuilder.build()`, ships them inside every
 * [[DatafusionSharedScanPartition]], and both the driver probe and the executors hand the same
 * values to `ScanBackend.createScan`, which builds the native `SessionContext` from them.
 *
 * `options` additionally disables the optimizer's plan-reshaping repartition passes so the
 * physical partitioning is exactly what the provider's `scan()` reports, on every machine.
 */
final case class PinnedSessionConfig(
    targetPartitions: Int,
    batchSize: Int,
    options: Vector[(String, String)]
) extends Serializable

object PinnedSessionConfig {

  val TargetPartitionsConf = "spark.datafusion.sharedScan.targetPartitions"
  val BatchSizeConf = "spark.datafusion.sharedScan.batchSize"
  val IdleTtlConf = "spark.datafusion.sharedScan.idleTtlMs"

  val DefaultTargetPartitions = 8
  val DefaultBatchSize = 8192
  val DefaultIdleTtlMs = 120000L

  /**
   * Optimizer knobs that must not vary with the host. Round-robin repartition and file-scan
   * repartition would let the optimizer change the plan's output partition count based on
   * `target_partitions` heuristics; statistics collection could steer per-host plan differences.
   */
  private val DeterminismOptions: Vector[(String, String)] = Vector(
    "datafusion.optimizer.enable_round_robin_repartition" -> "false",
    "datafusion.optimizer.repartition_file_scans" -> "false",
    "datafusion.execution.collect_statistics" -> "false"
  )

  /**
   * Resolve the pinned config from the driver's session conf. Called exactly once per scan, on
   * the driver; executors never read Spark conf for these values — they use the shipped copy.
   */
  def fromConf(conf: SQLConf): PinnedSessionConfig = {
    PinnedSessionConfig(
      targetPartitions =
        conf.getConfString(TargetPartitionsConf, DefaultTargetPartitions.toString).toInt,
      batchSize = conf.getConfString(BatchSizeConf, DefaultBatchSize.toString).toInt,
      options = DeterminismOptions
    )
  }

  def idleTtlMs(conf: SQLConf): Long =
    conf.getConfString(IdleTtlConf, DefaultIdleTtlMs.toString).toLong
}
