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
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Per-task `PartitionReader` factory. Columnar-only: row-based reads would force the connector
 * to convert Arrow → `InternalRow` per row, defeating the zero-copy path that is the whole
 * reason we are in-process.
 */
class DatafusionPartitionReaderFactory(val readSchema: StructType) extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException(
      "DatafusionPartitionReaderFactory: row-based read not supported; consumers must opt into columnar"
    )

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    partition match {
      case p: DatafusionInputPartition =>
        new DatafusionColumnarPartitionReader(p, readSchema)
      case p: DatafusionKeyedInputPartition =>
        new DatafusionColumnarPartitionReader(p.base, readSchema)
      case p: DatafusionSharedScanPartition =>
        new SharedScanPartitionReader(p, SharedScanCache.global)
      case other =>
        throw new IllegalArgumentException(
          s"unexpected InputPartition type: ${other.getClass.getName}")
    }
}
