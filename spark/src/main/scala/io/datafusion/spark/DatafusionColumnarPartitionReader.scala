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
import org.apache.datafusion.{DataFrame, SessionContext}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Per-task columnar reader for the per-partition payload (legacy) path. Lifecycle:
 *
 *   1. Reflectively instantiate the bridge's `FfiProviderFactory` (no-arg).
 *   2. `createProvider(optionsProtoBytes, partitionBytes)` — bridge builds an `Arc<dyn
 *      TableProvider>` materialising the slice described by `partitionBytes`, wraps it in an
 *      `FFI_TableProvider`, returns the raw pointer.
 *   3. Hand that pointer to connector-core's widening cdylib via `FfiHelperNative.wrapWithWidening`.
 *      The cdylib wraps the inner provider in a `WideningTableProvider` (kernel-level
 *      `arrow::compute::cast` for Spark-incompatible Arrow types) and re-FFIs it.
 *   4. Register the widened pointer on a fresh `SessionContext` via `registerFfiTable`.
 *   5. Build a `SELECT projection FROM <table>` DataFrame; apply pushed filters via
 *      `DataFrame.filterFromProto`, closing each intermediate frame.
 *   6. `executeStream` returns an `ArrowReader`; batches surface through
 *      [[ArrowColumnarBatchIteration]].
 */
class DatafusionColumnarPartitionReader(
    partition: DatafusionInputPartition,
    readSchema: StructType
) extends PartitionReader[ColumnarBatch]
    with ArrowColumnarBatchIteration {

  private val allocator = new RootAllocator(Long.MaxValue)
  private val ctx: SessionContext = new SessionContext()

  private val factory: FfiProviderFactory = instantiateFactory(partition.factoryFqcn)

  override protected val arrowReader: ArrowReader = {
    val rawPtr = factory.createProvider(partition.optionsProtoBytes, partition.partitionBytes)
    val widenedPtr = FfiHelperNative.wrapWithWidening(rawPtr)
    ctx.registerFfiTable(DatafusionSqlBuilder.PartitionTableName, widenedPtr)
    var df: DataFrame = ctx.sql(
      DatafusionSqlBuilder
        .buildSql(partition.projectionColumnNames, DatafusionSqlBuilder.PartitionTableName))
    var i = 0
    while (i < partition.filterProtoBytes.length) {
      val filtered = df.filterFromProto(partition.filterProtoBytes(i))
      df.close()
      df = filtered
      i += 1
    }
    df.executeStream(allocator)
  }

  override def close(): Unit = {
    var first: Throwable = null
    def safe(f: => Unit): Unit =
      try f
      catch { case t: Throwable => if (first == null) first = t else first.addSuppressed(t) }
    safe(arrowReader.close())
    safe(ctx.close())
    safe(allocator.close())
    if (first != null) throw first
  }

  private def instantiateFactory(fqcn: String): FfiProviderFactory = {
    val cls = Class.forName(fqcn)
    cls.getDeclaredConstructor().newInstance().asInstanceOf[FfiProviderFactory]
  }
}
