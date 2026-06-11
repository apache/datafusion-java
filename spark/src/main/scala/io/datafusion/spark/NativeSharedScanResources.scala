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

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.datafusion.{DataFrame, PartitionedExecution, SessionContext}
import org.apache.spark.internal.Logging

/**
 * JNI-backed shared-scan entry: one provider, one `SessionContext`, one planned
 * [[PartitionedExecution]].
 *
 * The build sequence is the single code path for BOTH the driver-side partition-count probe and
 * every executor's cache entry — identical widening, registration, SQL, filters, and pinned
 * session config are what make the partition count comparable across machines (the bridge's
 * determinism contract covers the rest).
 */
private[spark] final class NativeSharedScanResources(
    allocator: RootAllocator,
    ctx: SessionContext,
    execution: PartitionedExecution
) extends SharedScanResources {

  override def partitionCount: Int = execution.partitionCount()

  override def newTaskAllocator(name: String): BufferAllocator =
    allocator.newChildAllocator(name, 0, Long.MaxValue)

  override def openPartitionStream(
      partition: Int,
      taskAllocator: BufferAllocator): ArrowReader =
    execution.executeStream(partition, taskAllocator)

  override def close(): Unit = {
    var first: Throwable = null
    def safe(f: => Unit): Unit =
      try f
      catch { case t: Throwable => if (first == null) first = t else first.addSuppressed(t) }
    safe(execution.close())
    safe(ctx.close())
    safe(allocator.close())
    if (first != null) throw first
  }
}

private[spark] object NativeSharedScanResources extends Logging {

  def build(spec: SharedScanSpec): SharedScanResources = {
    logInfo(
      s"Building shared-scan entry for scanId=${spec.scanId} " +
        s"(factory=${spec.factoryFqcn}, filters=${spec.filterProtoBytes.length})")

    val factory = Class
      .forName(spec.factoryFqcn)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[FfiProviderFactory]

    val allocator = new RootAllocator(Long.MaxValue)
    var ctx: SessionContext = null
    try {
      // Shared mode builds the dataset-wide provider: empty partitionBytes, like the
      // driver-side schema probe. DataFusion-native partitioning replaces listPartitions.
      val rawPtr = factory.createProvider(spec.optionsProtoBytes, Array.emptyByteArray)
      val widenedPtr = FfiHelperNative.wrapWithWidening(rawPtr)

      ctx = spec.pinnedConfig.buildContext()
      ctx.registerFfiTable(DatafusionSqlBuilder.SharedTableName, widenedPtr)

      var df: DataFrame = ctx.sql(
        DatafusionSqlBuilder
          .buildSql(spec.projectionColumnNames, DatafusionSqlBuilder.SharedTableName))
      var i = 0
      while (i < spec.filterProtoBytes.length) {
        val filtered = df.filterFromProto(spec.filterProtoBytes(i))
        df.close()
        df = filtered
        i += 1
      }

      val execution = df.toPartitionedExecution()
      new NativeSharedScanResources(allocator, ctx, execution)
    } catch {
      case t: Throwable =>
        if (ctx != null) {
          try ctx.close()
          catch { case suppressed: Throwable => t.addSuppressed(suppressed) }
        }
        try allocator.close()
        catch { case suppressed: Throwable => t.addSuppressed(suppressed) }
        throw t
    }
  }
}
