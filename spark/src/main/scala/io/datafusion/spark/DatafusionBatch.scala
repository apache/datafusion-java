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

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

/**
 * Spark `Batch` for a DataFusion-backed scan. Owns:
 *   - partition planning (driver-side: `factory.listPartitions` enumerates partition ids → one
 *     task per id)
 *   - per-task reader factory ([[DatafusionPartitionReaderFactory]])
 */
class DatafusionBatch(val scan: DatafusionScan) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    val factory = instantiateFactory(scan.factoryFqcn)
    val partitionIds: Array[String] = factory.listPartitions(scan.optionsProtoBytes)

    if (partitionIds == null || partitionIds.isEmpty) {
      throw new IllegalStateException(
        s"FfiProviderFactory '${scan.factoryFqcn}' returned no partitions to scan"
      )
    }

    val projection = scan.prunedSchema.fieldNames
    val filterBytes: Array[Array[Byte]] = scan.pushedPredicateBytes

    partitionIds.iterator.map { id =>
      DatafusionInputPartition(
        factoryFqcn = scan.factoryFqcn,
        optionsProtoBytes = scan.optionsProtoBytes,
        projectionColumnNames = projection,
        filterProtoBytes = filterBytes,
        partitionId = id
      ).asInstanceOf[InputPartition]
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new DatafusionPartitionReaderFactory(scan.prunedSchema)

  private def instantiateFactory(fqcn: String): FfiProviderFactory = {
    val cls = Class.forName(fqcn)
    cls.getDeclaredConstructor().newInstance().asInstanceOf[FfiProviderFactory]
  }
}
