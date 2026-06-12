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

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.TaskContext
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Shared-scan task reader: acquires the executor's cached (provider, plan) entry and streams ONE
 * DataFusion plan partition from it. The acquire/release refcount pair brackets the reader's
 * whole lifetime, so the cache can never close the native plan under an open stream.
 */
class SharedScanPartitionReader(
    partition: DatafusionSharedScanPartition,
    cache: SharedScanCache
) extends PartitionReader[ColumnarBatch]
    with ArrowColumnarBatchIteration {

  private val resources: SharedScanResources = cache.acquire(partition.toSpec, partition.idleTtlMs)

  // Determinism guard: the driver counted partitions by planning once; if this executor's
  // re-plan disagrees, partition indices are meaningless and every task of the scan must fail
  // rather than silently drop or duplicate data.
  if (resources.partitionCount != partition.numPartitions) {
    val executorCount = resources.partitionCount
    cache.release(partition.scanId)
    throw new IllegalStateException(
      s"shared-scan determinism violation for scanId=${partition.scanId}: driver planned " +
        s"${partition.numPartitions} partition(s) but this executor planned $executorCount. " +
        "The provider's partitioning must be a pure function of optionsBytes; pin your " +
        "source snapshot (see BridgeProviderFactory.sharedScan).")
  }

  private val taskAllocator: BufferAllocator = {
    val attempt = Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
    resources.newTaskAllocator(
      s"shared-${partition.scanId}-p${partition.partitionIndex}-attempt$attempt")
  }

  override protected val arrowReader: ArrowReader =
    try {
      resources.openPartitionStream(partition.partitionIndex, taskAllocator)
    } catch {
      case t: Throwable =>
        try taskAllocator.close()
        catch { case suppressed: Throwable => t.addSuppressed(suppressed) }
        cache.release(partition.scanId)
        throw t
    }

  override def close(): Unit = {
    var first: Throwable = null
    def safe(f: => Unit): Unit =
      try f
      catch { case t: Throwable => if (first == null) first = t else first.addSuppressed(t) }
    safe(arrowReader.close())
    safe(taskAllocator.close())
    // Release LAST: the refcount must cover the open stream and the task allocator.
    safe(cache.release(partition.scanId))
    if (first != null) throw first
  }
}
