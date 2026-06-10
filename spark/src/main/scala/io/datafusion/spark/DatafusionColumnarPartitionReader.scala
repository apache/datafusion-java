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
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.datafusion.{DataFrame, SessionContext}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/**
 * Per-task columnar reader. Lifecycle:
 *
 *   1. Reflectively instantiate the bridge's `FfiProviderFactory` (no-arg).
 *   2. `createProvider(optionsProtoBytes)` — bridge builds an `Arc<dyn TableProvider>`, wraps it
 *      in an `FFI_TableProvider`, returns the raw pointer.
 *   3. Hand that pointer to connector-core's widening cdylib via `FfiHelperNative.wrapWithWidening`.
 *      The cdylib wraps the inner provider in a `WideningTableProvider` (kernel-level
 *      `arrow::compute::cast` for Spark-incompatible Arrow types) and re-FFIs it.
 *   4. Register the widened pointer on a fresh `SessionContext` via `registerFfiTable`.
 *   5. Build a `SELECT projection FROM <table>` DataFrame; apply pushed filters via
 *      `DataFrame.filterFromProto`.
 *   6. `executeStream` returns an `ArrowReader`; each `loadNextBatch()` yields a
 *      `VectorSchemaRoot` we wrap as a `ColumnarBatch` of `NonClosingArrowColumnVector`s.
 */
class DatafusionColumnarPartitionReader(
    partition: DatafusionInputPartition,
    readSchema: StructType
) extends PartitionReader[ColumnarBatch] {

  private val TableName = "df_spark_partition"

  private val allocator = new RootAllocator(Long.MaxValue)
  private val ctx: SessionContext = new SessionContext()

  private val factory: FfiProviderFactory = instantiateFactory(partition.factoryFqcn)

  private val df: DataFrame = {
    val rawPtr = factory.createProvider(partition.optionsProtoBytes)
    val widenedPtr = FfiHelperNative.wrapWithWidening(rawPtr)
    ctx.registerFfiTable(TableName, widenedPtr)
    var d = ctx.sql(buildSql())
    var i = 0
    while (i < partition.filterProtoBytes.length) {
      d = d.filterFromProto(partition.filterProtoBytes(i))
      i += 1
    }
    d
  }
  private val reader: ArrowReader = df.executeStream(allocator)

  private var currentBatch: ColumnarBatch = _

  override def next(): Boolean = {
    if (currentBatch != null) {
      currentBatch = null
    }
    if (!reader.loadNextBatch()) return false
    val root = reader.getVectorSchemaRoot
    val vectors: java.util.List[FieldVector] = root.getFieldVectors
    val cols = new Array[ColumnVector](vectors.size())
    var i = 0
    while (i < vectors.size()) {
      cols(i) = new NonClosingArrowColumnVector(vectors.get(i))
      i += 1
    }
    val batch = new ColumnarBatch(cols)
    batch.setNumRows(root.getRowCount)
    currentBatch = batch
    true
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    var first: Throwable = null
    def safe(f: => Unit): Unit =
      try f
      catch { case t: Throwable => if (first == null) first = t else first.addSuppressed(t) }
    safe(reader.close())
    safe(ctx.close())
    safe(allocator.close())
    if (first != null) throw first
  }

  private def buildSql(): String = {
    val cols =
      if (partition.projectionColumnNames.isEmpty) "*"
      else
        partition.projectionColumnNames
          .map(c => "\"" + c.replace("\"", "\"\"") + "\"")
          .mkString(", ")
    s"""SELECT $cols FROM "$TableName""""
  }

  private def instantiateFactory(fqcn: String): FfiProviderFactory = {
    val cls = Class.forName(fqcn)
    cls.getDeclaredConstructor().newInstance().asInstanceOf[FfiProviderFactory]
  }

}
