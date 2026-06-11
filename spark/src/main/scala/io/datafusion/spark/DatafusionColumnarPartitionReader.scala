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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Per-task columnar reader for the per-partition payload (legacy) path. Lifecycle:
 *
 *   1. Reflectively instantiate the bridge's `FfiProviderFactory` (no-arg) and take its
 *      [[ScanBackend]].
 *   2. `backend.createScan(options, partitionBytes, ...)` — builds the provider for the slice
 *      described by `partitionBytes` and does the rest natively: widening wrap, private
 *      `SessionContext`, projection, pushed proto filters, physical plan.
 *   3. `backend.executeStream` streams the whole plan (the provider already IS the task's
 *      slice); batches surface through [[ArrowColumnarBatchIteration]].
 */
class DatafusionColumnarPartitionReader(
    partition: DatafusionInputPartition,
    readSchema: StructType
) extends PartitionReader[ColumnarBatch]
    with ArrowColumnarBatchIteration {

  private val allocator = new RootAllocator(Long.MaxValue)

  private val backend: ScanBackend = instantiateFactory(partition.factoryFqcn).scanBackend()

  private val scanHandle: Long =
    try {
      backend.createScan(
        partition.optionsProtoBytes,
        partition.partitionBytes,
        /* targetPartitions = */ -1,
        /* batchSize = */ -1,
        Array.empty[String],
        Array.empty[String],
        partition.projectionColumnNames,
        partition.filterProtoBytes
      )
    } catch {
      case t: Throwable =>
        try allocator.close()
        catch { case suppressed: Throwable => t.addSuppressed(suppressed) }
        throw t
    }

  override protected val arrowReader: ArrowReader =
    try {
      FfiStream.importReader(allocator) { addr =>
        backend.executeStream(scanHandle, addr)
      }
    } catch {
      case t: Throwable =>
        try backend.closeScan(scanHandle)
        catch { case suppressed: Throwable => t.addSuppressed(suppressed) }
        try allocator.close()
        catch { case suppressed: Throwable => t.addSuppressed(suppressed) }
        throw t
    }

  override def close(): Unit = {
    var first: Throwable = null
    def safe(f: => Unit): Unit =
      try f
      catch { case t: Throwable => if (first == null) first = t else first.addSuppressed(t) }
    safe(arrowReader.close())
    safe(backend.closeScan(scanHandle))
    safe(allocator.close())
    if (first != null) throw first
  }

  private def instantiateFactory(fqcn: String): FfiProviderFactory = {
    val cls = Class.forName(fqcn)
    cls.getDeclaredConstructor().newInstance().asInstanceOf[FfiProviderFactory]
  }
}
